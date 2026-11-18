"""Contract equality and sampled-path differences have separate captured comparisons."""

import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from almanak.backtest.dataset import RawEvidence
from almanak.connectors.aerodrome.economic_book import RetainedReader
from almanak.connectors.aerodrome.economic_data import Record
from almanak.connectors.aerodrome.economic_simulation import quote_at
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.reader import quote
from almanak.models import AssetAmount, Block

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/aerodrome-base-fidelity"
CAPTURE = json.loads(gzip.decompress((EVIDENCE / "comparison.json.gz").read_bytes()))


@pytest.mark.parametrize("invariant", ["stable", "volatile"])
def test_historical_router_and_sampled_quote_comparisons_reproduce_offline(tmp_path, invariant):
    path = tmp_path / "observations.sqlite"
    path.write_bytes(
        gzip.decompress((EVIDENCE / invariant / "observations.sqlite.gz").read_bytes())
    )
    with sqlite3.connect(path) as database:
        raw = tuple(
            RawEvidence(body=r[0]) for r in database.execute("SELECT body FROM observations")
        )
    rows = [r for r in CAPTURE["rows"] if r["invariant"] == invariant]
    target = Target.model_validate_json(json.dumps(rows[0]["target"]))
    reader = RetainedReader(target.fund.chain, raw)
    assert len(rows) == 40
    for row in rows:
        record = Record.model_validate_json(json.dumps(row["sample"]))
        amount = AssetAmount.model_validate_json(json.dumps(row["amount"]))
        block = Block.model_validate_json(json.dumps(row["block"]))
        modeled = quote_at(record, target, amount, record.available_at)
        assert modeled.amount_out.model_dump(mode="json") == row["modeled"]
        assert reader.pin_block(block.number) == block
        actual = quote(
            reader, target.pool, target.contracts, block, amount, modeled.amount_out.asset
        )
        assert actual == int(row["router_output"])
        assert modeled.amount_out.amount - actual == int(row["difference"])
        if row["kind"] == "same_block":
            assert actual == modeled.amount_out.amount and record.reference == block
        else:
            assert row["age_seconds"] == row["time"] - record.source_time
            assert record.available_at <= row["time"] <= record.available_at + 30
