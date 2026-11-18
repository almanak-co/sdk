"""Pancake prepared samples reproduce frozen calls and compiled core fee accounting."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.pancakeswap_v3.economic_acquisition import sample
from almanak.connectors.pancakeswap_v3.economic_data import PoolSample, PreparedSample
from almanak.connectors.pancakeswap_v3.fee_growth import accrued, inside
from almanak.connectors.pancakeswap_v3.swap_math import exact_input
from tests.test_pancakeswap_v3_reader import CAPTURE, Replay

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/pancakeswap-v3-bnb-pool-model"
)


def retained(name):
    return json.loads(gzip.decompress((EVIDENCE / f"{name}.json.gz").read_bytes()))


def prepared():
    ticks = retained("full-ticks")
    comparisons = retained("full-comparisons")
    growth = json.loads(
        gzip.decompress(
            (
                Path(__file__).with_name("fixtures") / "pancakeswap-v3-global-growth.json.gz"
            ).read_bytes()
        )
    )
    row = CAPTURE[2]
    assert row["block"] == ticks["block"] == comparisons["block"] == growth["block"]
    with Replay(
        {
            **row,
            "requests": [
                *row["requests"],
                *ticks["requests"],
                *comparisons["requests"],
                *growth["requests"],
            ],
        }
    ) as reader:
        block = reader.pin_block(row["block"]["number"])
        return sample(
            reader,
            reader.source,
            block,
            lower=-887272,
            upper=887272,
            max_words=140,
            max_ticks=1024,
        )


def test_prepared_acquisition_and_offline_swaps_match_archival_pancake_quoter():
    payload = prepared()
    assert PoolSample.model_validate_json(payload.model_dump_json()) == payload
    crossed = []
    for row in retained("full-comparisons")["comparisons"]:
        result = exact_input(
            payload,
            row["amount"]["amount"],
            row["amount"]["asset"] == str(payload.pool.source.currency0),
        )
        assert result.output == row["quoter"][0]
        assert result.sqrt_price_x96 == row["quoter"][1]
        assert result.tick == row["model"]["tick"]
        assert result.liquidity == row["model"]["liquidity"]
        crossed.extend(result.crossed_ticks)
    assert crossed


def test_prepared_schema_rejects_missing_ticks():
    payload = prepared().model_dump(mode="json")
    payload["ticks"] = []
    with pytest.raises(ValueError, match="does not match"):
        PoolSample.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("method", ["inside", "earned"])
def test_fee_accounting_matches_compiled_pancake_core(method):
    for row in retained("fee-vectors")["records"]:
        if row["method"] != method:
            continue
        result = inside(*row["args"]) if method == "inside" else (accrued(*row["args"]),)
        assert result == tuple(row["result"])


@pytest.mark.parametrize("network", ["ethereum", "base", "arbitrum"])
def test_verified_callback_markets_match_archival_quoter_at_prepared_sample(network):
    from almanak.connectors.pancakeswap_v3.domain import Contracts
    from almanak.connectors.pancakeswap_v3.reader import quote
    from almanak.models import AssetAmount

    directory = EVIDENCE.with_name("pancakeswap-v3-additional-prepared")
    captured = json.loads(
        gzip.decompress((directory / f"{network}-model-comparison.json.gz").read_bytes())
    )
    payload = PreparedSample.model_validate_json(json.dumps(captured["record"]["payload"]))
    source = payload.pool.source
    contracts = Contracts.model_validate_json(json.dumps(captured["contracts"]))
    crossed = []
    with Replay({"source": source.model_dump(mode="json"), "requests": captured["requests"]}) as r:
        block = r.pin_block(payload.pool.block.number)
        assert block == payload.pool.block
        for row in captured["comparisons"]:
            amount = AssetAmount.model_validate_json(json.dumps(row["amount"]))
            direction = amount.asset == source.currency0
            output = source.currency1 if direction else source.currency0
            actual = quote(r, source, contracts, block, amount, output)
            result = exact_input(payload, amount.amount, direction)
            assert actual == tuple(row["quoter"])
            assert (result.output, result.sqrt_price_x96) == actual[:2]
            crossed.extend(result.crossed_ticks)
        assert r.seen == set(r.records)
    assert len(captured["comparisons"]) == 12
    assert crossed
