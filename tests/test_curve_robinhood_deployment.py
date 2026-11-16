"""Pinned Curve core deployments require exact bytecode and revision-specific storage."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.curve import crypto_deployment, deployment, tricrypto_deployment
from almanak.connectors.curve.crypto_reader import read_pool as two
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.curve.reader import read_pool as stable
from almanak.connectors.curve.tricrypto_reader import read_pool as tri
from almanak.connectors.evm_revision import verify_code
from almanak.gateway.ethereum import DataError
from tests.test_curve_reader import CapturedReader

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-discovery"
)


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


@pytest.mark.parametrize("index,read", [(0, stable), (1, two), (2, tri)])
def test_core_readers_reproduce_pinned_observations(index, read):
    data = captured("reader-probe")
    pool = data["pools"][index]
    source = CurvePool.model_validate_json(json.dumps(pool["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["block"]["number"])
        assert read(reader, source, block).model_dump(mode="json") == pool["observation"]


@pytest.mark.parametrize("index", range(15))
def test_core_components_match_compiled_source_and_reject_mutation(index):
    data = captured("component-verification")
    contract = data["contracts"][index]
    compiled = {
        "stable": deployment.revisions()[deployment.CORE],
        "two": crypto_deployment.revision("twocrypto-core-v2.1.0.json"),
        "tri": tricrypto_deployment.revision("tricrypto-core-v2.0.0.json"),
    }[contract["family"]]
    raw = next(
        r["result"]
        for r in data["records"]
        if r["method"] == "eth_getCode" and r["params"][0] == contract["address"]
    )
    assert verify_code(raw, compiled[contract["component"]]) == contract["immutables"]
    with pytest.raises(DataError, match="compiled"):
        verify_code("0x00" + raw[4:], compiled[contract["component"]])


def test_core_two_factory_uses_its_compiled_slot_and_refuses_legacy_slot():
    data = captured("reader-probe")
    source = CurvePool.model_validate_json(json.dumps(data["pools"][1]["source"]))
    compiled = crypto_deployment.revision("twocrypto-core-v2.1.0.json")
    assert compiled["factory_storage_layout"]["pool_data"]["slot"] == 8
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["block"]["number"])
        result = crypto_deployment.verify_pool(reader, source, block)
        assert (
            crypto_deployment.original_blueprint(reader, source, block, pool_data_slot=8)
            == result.blueprint.address
        )
        with pytest.raises(KeyError):
            crypto_deployment.original_blueprint(reader, source, block, pool_data_slot=9)


def test_core_tri_pool_layout_matches_the_january_economic_revision():
    old = tricrypto_deployment.revision("tricrypto-2024-01-04.json")
    new = tricrypto_deployment.revision("tricrypto-core-v2.0.0.json")
    assert new["pool_storage_layout"] == old["pool_storage_layout"]


def economic_source(source):
    import re

    # Module documentation and the public version getter differ. The version
    # also changes permit-domain metadata; these LP proofs use explicit approvals.
    source = re.sub(r'(?s)\A(.*?)""".*?"""', r"\1", source, count=1)
    return tuple(
        line.strip()
        for line in source.splitlines()
        if line.strip()
        and not line.lstrip().startswith("#")
        and not line.startswith("version: public(constant(String[8]))")
    )


@pytest.mark.parametrize("family", ["stable", "two", "tri"])
def test_core_economic_source_matches_the_accepted_model_lineage(family):
    versions = [
        gzip.decompress((EVIDENCE / f"{family}-{kind}.vy.gz").read_bytes()).decode()
        for kind in ("model", "core")
    ]
    assert economic_source(versions[0]) == economic_source(versions[1])


def test_sampled_archive_dust_comparison_uses_measured_contract_zero():
    from almanak.caip import AssetType
    from almanak.models import AssetAmount
    from tests.qa.curve_comparisons import contract_quote

    data = captured("qa-comparison")
    cases = [row for row in data["comparisons"] if row["contract"] == 0]
    assert len(cases) == 12
    for row in cases:
        source = CurvePool.model_validate_json(json.dumps(row["pool"]))
        amount = AssetAmount.model_validate_json(json.dumps(row["input"]))
        with CapturedReader(source, data["records"]) as reader:
            block = reader.pin_block(row["block"]["number"])
            assert (
                contract_quote(reader, source, block, amount, AssetType(row["output_asset"]), 0)
                == 0
            )
