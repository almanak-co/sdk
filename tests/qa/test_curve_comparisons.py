"""Captured archival responses reproduce the Curve QA comparison without providers."""

import gzip
import json
import sqlite3
from contextlib import nullcontext
from types import SimpleNamespace

import httpx
import pytest

from almanak.connectors.curve.economic_binding import Manifest
from tests.qa import curve_comparisons as comparisons
from tests.qa.catalog import ROOT
from tests.qa.retained import current_json
from tests.test_curve_reader import CapturedReader

EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/curve-public-qa/mixed"


def restore(directory, source, names):
    directory.mkdir(exist_ok=True)
    for name in names:
        (directory / name).write_bytes(gzip.decompress((source / (name + ".gz")).read_bytes()))


@pytest.fixture
def captured(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("archival comparison replay attempted network access")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    dataset, run = tmp_path / "dataset", tmp_path / "run"
    restore(dataset, EVIDENCE / "dataset", ("dataset.json", "data.sqlite", "observations.sqlite"))
    restore(run, EVIDENCE / "run", ("manifest.json",))
    manifest = Manifest.model_validate_json(current_json((run / "manifest.json").read_text()))
    (run / "manifest.json").write_text(
        manifest.model_copy(update={"dataset": dataset}).model_dump_json()
    )
    with sqlite3.connect(dataset / "observations.sqlite") as db:
        evidence = [json.loads(row[0]) for row in db.execute("SELECT body FROM observations")]
    records = [{**row["request"], "result": row["result"]} for row in evidence]
    monkeypatch.setattr(
        comparisons, "GatewayProcess", lambda *args: nullcontext(SimpleNamespace(endpoint="unused"))
    )
    monkeypatch.setattr(comparisons, "read_token", lambda *args: "unused")
    monkeypatch.setattr(
        comparisons,
        "HistoricalReader",
        lambda chain, *args: CapturedReader(SimpleNamespace(chain=chain), records),
    )
    return tmp_path


def test_retained_contract_reads_reproduce_all_ninety_exact_comparisons(captured):
    comparisons.compare_pool(captured, None)
    actual = json.loads((captured / "curve-contract-comparisons.json").read_text())
    expected = json.loads(
        gzip.decompress((EVIDENCE / "curve-contract-comparisons.json.gz").read_bytes())
    )
    assert actual == expected and len(actual) == 90
    assert {row["family"] for row in actual} == {"stableswap_ng", "twocrypto_ng", "tricrypto_ng"}


def test_comparison_failure_is_retained_and_cannot_pass(captured, monkeypatch):
    original = comparisons.quote
    monkeypatch.setattr(comparisons, "quote", lambda *args: original(*args) + 1)
    with pytest.raises(AssertionError):
        comparisons.compare_pool(captured, None)
    records = json.loads((captured / "curve-contract-comparisons.json").read_text())
    assert records and all(not row["equal"] for row in records)


@pytest.mark.parametrize("modeled,kind", [(1, "liquidity"), (0, "unavailable")])
def test_dust_comparison_cannot_turn_other_failures_into_zero(monkeypatch, modeled, kind):
    from almanak.gateway.ethereum import DataError

    def refuse(*args):
        raise DataError(kind, "retained refusal")

    monkeypatch.setattr(comparisons, "quote", refuse)
    with pytest.raises(DataError, match="retained refusal"):
        comparisons.contract_quote(None, None, None, None, None, modeled)
