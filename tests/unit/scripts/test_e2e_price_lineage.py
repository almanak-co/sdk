"""One captured consumption record; this fixture makes no continuity claim."""

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from qa_lab.e2e_price_lineage import price_lineage

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-price-input"


@pytest.fixture
def bundle(tmp_path):
    shutil.copytree(FIXTURE, tmp_path, dirs_exist_ok=True)
    return tmp_path


def check(bundle):
    return price_lineage(
        bundle / "run.log", bundle / "price-observations", bundle / "pool-input.json", max_age_seconds=60
    )


def test_captured_strategy_consumed_exact_gateway_pool_bytes(bundle):
    result = check(bundle)
    assert result["status"] == "PASS"
    assert result["deployment_id"] == "deployment:4a9f5de1c786"
    assert len(result["records"]) == 1
    assert len(result["records"][0]["inputs"]) == 2


@pytest.mark.parametrize(
    "fault",
    ["price", "missing", "hash", "rehash_fee", "rehash_factory", "provider", "stale", "future", "naive", "fork"],
)
def test_consumption_mutations_fail_closed(bundle, fault):
    log = bundle / "run.log"
    text = log.read_text()
    record, _ = json.JSONDecoder().raw_decode(text.split("LP_PRICE_INPUT ", 1)[1])
    identity = record["token0_observation_id"]
    path = bundle / "price-observations" / f"{identity}.json"
    observation = json.loads(path.read_text())
    if fault == "price":
        log.write_text(text.replace(record["token0_price"], "1"))
    elif fault == "missing":
        path.unlink()
    elif fault == "hash":
        path.write_text("{}")
    else:
        if fault == "rehash_fee":
            observation["raw_reads"]["fee"] = "0x" + f"{3000:064x}"
        elif fault == "rehash_factory":
            observation["raw_reads"]["factory_pool"] = "0x" + "00" * 32
        elif fault == "provider":
            observation["source"] = "manual_override"
        elif fault == "stale":
            observation["observed_at"] = "2026-09-07T00:00:00+00:00"
        elif fault == "future":
            observation["observed_at"] = "2026-09-08T00:00:00+00:00"
        elif fault == "fork":
            observation["fork_identity"]["instance_id"] = "another-fork"
        else:
            observation["observed_at"] = "2026-09-07T00:00:00"
        raw = json.dumps(observation).encode()
        changed = hashlib.sha256(raw).hexdigest()
        (path.parent / f"{changed}.json").write_bytes(raw)
        log.write_text(text.replace(identity, changed))
    result = check(bundle)
    assert result["status"] == "FAIL"
    assert {
        "price": "differs from the raw",
        "missing": "owned regular JSON",
        "hash": "bytes differ",
        "rehash_fee": "units or fee",
        "rehash_factory": "factory response",
        "provider": "different provider",
        "stale": "stale",
        "future": "observed after",
        "naive": "timezone",
        "fork": "retained fork binding",
    }[fault] in result["reason"]


def test_no_consumption_records_are_unmeasured(bundle):
    (bundle / "run.log").write_text("runner booted\n")
    assert check(bundle)["status"] == "UNMEASURED"


def test_price_lineage_requires_explicit_freshness_policy(bundle):
    with pytest.raises(ValueError, match="freshness"):
        price_lineage(bundle / "run.log", bundle / "price-observations", bundle / "pool-input.json", max_age_seconds=0)


def test_legacy_price_lineage_does_not_certify_confidence_or_chain_freshness(bundle):
    result = check(bundle)
    assert result["status"] == "PASS"
    assert result["assumptions"]["confidence_calibration"] == "UNMEASURED"
    assert result["assumptions"]["chain_head_freshness"] == "UNMEASURED"
    assert all(item["measurement_policy"] is None for item in result["records"][0]["inputs"])
    required = price_lineage(
        bundle / "run.log",
        bundle / "price-observations",
        bundle / "pool-input.json",
        max_age_seconds=60,
        require_measurement_policy=True,
    )
    assert required["status"] == "FAIL"
    assert "measurement policy" in required["reason"]


@pytest.mark.parametrize("mutation", [None, "confidence", "stale", "freshness", "boolean", "missing"])
def test_rehashed_price_policy_cannot_promote_experiment_assumptions(bundle, mutation):
    from qa_lab.e2e_price_lineage import MEASUREMENT_POLICY

    log = bundle / "run.log"
    text = log.read_text()
    record, _ = json.JSONDecoder().raw_decode(text.split("LP_PRICE_INPUT ", 1)[1])
    for index in range(2):
        old_id = record[f"token{index}_observation_id"]
        directory = bundle / "price-observations"
        observation = json.loads((directory / f"{old_id}.json").read_text())
        policy = dict(MEASUREMENT_POLICY)
        if mutation == "confidence":
            policy["confidence_basis"] = "independently_calibrated"
        elif mutation == "stale":
            policy["stale"] = True
        elif mutation == "freshness":
            policy["freshness_basis"] = "chain_head_is_current"
        elif mutation == "boolean":
            policy["stale"] = 0
        elif mutation == "missing":
            policy.pop("confidence_basis")
        observation["measurement_policy"] = policy
        raw = json.dumps(observation).encode()
        changed = hashlib.sha256(raw).hexdigest()
        (directory / f"{changed}.json").write_bytes(raw)
        text = text.replace(old_id, changed)
    log.write_text(text)
    result = price_lineage(
        log,
        bundle / "price-observations",
        bundle / "pool-input.json",
        max_age_seconds=60,
        require_measurement_policy=True,
    )
    assert result["status"] == ("PASS" if mutation is None else "FAIL")
    if mutation is None:
        assert result["assumptions"]["confidence_calibration"] == "UNMEASURED"
        assert all(item["measurement_policy"] == MEASUREMENT_POLICY for item in result["records"][0]["inputs"])
    else:
        assert "measurement policy" in result["reason"]


def test_new_frozen_price_contract_requires_metadata_from_gateway(bundle):
    from qa_lab.e2e_price_lineage import validate_price_contract

    cell = {
        "strategy_path": "strategies/accounting/lp_dual",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "primitive": "lp",
        "network": "anvil",
        "exec_path": "eoa",
    }
    assert (
        validate_price_contract(
            bundle,
            {"pool_price_inputs": {"schema_version": 1, "max_age_seconds": 60}},
            catalog_cell=cell,
        )["status"]
        == "PASS"
    )
    with pytest.raises(ValueError, match="measurement policy"):
        validate_price_contract(
            bundle,
            {"pool_price_inputs": {"schema_version": 2, "max_age_seconds": 60}},
            catalog_cell=cell,
        )
