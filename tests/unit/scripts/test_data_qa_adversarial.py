"""Adversarial contracts for Data QA evidence sealing.

These tests intentionally exercise bundle shapes that look plausible while lacking
the exact subject, provenance, or freshness needed for an official PASS.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
QA_COVERAGE_PATH = REPO_ROOT / "scripts" / "quant-test" / "qa_coverage.py"


def _load_qa_coverage():
    module_name = "qa_coverage_data_adversarial"
    loaded = sys.modules.get(module_name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(module_name, QA_COVERAGE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def qa():
    return _load_qa_coverage()


def _identity(*, pair: str = "WETH/USD", address_byte: str = "12") -> dict[str, object]:
    return {
        "provider": "chainlink",
        "chain": "arbitrum",
        "address": "0x" + address_byte * 20,
        "pair": pair,
        "decimals": 8,
        "kind": "usd",
    }


def _write_identity_records(bundle: Path, identities: list[dict[str, object]]) -> None:
    records = [
        {
            "declared": identity,
            "authority": {
                "identity": identity,
                "authority_uri": "https://data.chain.link/arbitrum/mainnet/crypto-usd/weth-usd",
            },
        }
        for identity in identities
    ]
    (bundle / "resource-identities.json").write_text(json.dumps({"records": records}), encoding="utf-8")


def _provider_summary(*, confidence: object = 0.99, observed_at: object = "2026-08-15T12:00:00Z") -> dict:
    return {
        "timestamp_utc": "2026-08-15T11:59:00Z",
        "completed_at_utc": "2026-08-15T12:00:00Z",
        "metrics": {
            "prices": {"WETH": "3000"},
            "providers": {
                "price_sources": {
                    "WETH": {
                        "source": "chainlink",
                        "confidence": confidence,
                        "stale": False,
                        "observed_at": observed_at,
                        "pair": "WETH/USD",
                    }
                },
                "ohlcv_source": {
                    "source": "binance",
                    "confidence": 0.99,
                    "stale": False,
                    "candle_count": 120,
                    "observed_at": observed_at,
                    "pair": "WETH/USD",
                    "timeframe": "4h",
                    "first_candle_at": "2026-07-01T00:00:00Z",
                    "last_candle_at": "2026-08-15T08:00:00Z",
                    "timestamps_strictly_increasing": True,
                },
            },
        },
        "checks": {"provider_attribution": {"ok": True}},
        "errors": [],
    }


def test_missing_identity_observation_is_unmeasured(qa, tmp_path: Path) -> None:
    """Absence is not evidence and must never be promoted to PASS."""
    status, reasons, count = qa._data_resource_identity_status(tmp_path, chain="arbitrum")

    assert status == "UNMEASURED"
    assert reasons == ["authoritative_resource_identity_absent"]
    assert count == 0


def test_unrelated_valid_identity_cannot_satisfy_provider_attribution(qa, tmp_path: Path) -> None:
    """A valid SCAM/USD record must not certify the observed WETH/USD source."""
    _write_identity_records(tmp_path, [_identity(pair="SCAM/USD")])

    derived = qa._derive_data_check_statuses(
        _provider_summary(),
        bundle=tmp_path,
        expected_checks={"provider_attribution"},
        chain="arbitrum",
        expected_price_subjects={"WETH"},
    )

    assert derived["provider_attribution"]["status"] != "PASS"


def test_duplicate_identity_observations_are_rejected(qa, tmp_path: Path) -> None:
    identity = _identity()
    _write_identity_records(tmp_path, [identity, identity])

    status, reasons, count = qa._data_resource_identity_status(tmp_path, chain="arbitrum")

    assert status == "FAIL"
    assert "duplicate_resource_identity" in reasons
    assert count == 1


def test_incomplete_price_subject_set_cannot_paint_chain_price_sanity(qa, tmp_path: Path) -> None:
    """The nightly contract names twelve subjects; observing only WETH is partial."""
    summary = {
        "metrics": {"prices": {"WETH": "3000"}},
        "checks": {"price_sanity": {"ok": True}},
        "errors": [],
    }

    derived = qa._derive_data_check_statuses(
        summary,
        bundle=tmp_path,
        expected_checks={"price_sanity"},
        chain="arbitrum",
    )

    assert derived["price_sanity"]["status"] != "PASS"


@pytest.mark.parametrize("confidence", [-0.01, 1.01, "high", None])
def test_invalid_provider_confidence_cannot_pass_attribution(qa, tmp_path: Path, confidence: object) -> None:
    _write_identity_records(tmp_path, [_identity()])

    derived = qa._derive_data_check_statuses(
        _provider_summary(confidence=confidence),
        bundle=tmp_path,
        expected_checks={"provider_attribution"},
        chain="arbitrum",
        expected_price_subjects={"WETH"},
    )

    assert derived["provider_attribution"]["status"] == "UNMEASURED"


def test_invalid_provider_observation_timestamp_cannot_pass_attribution(qa, tmp_path: Path) -> None:
    _write_identity_records(tmp_path, [_identity()])

    derived = qa._derive_data_check_statuses(
        _provider_summary(observed_at="not-an-iso8601-timestamp"),
        bundle=tmp_path,
        expected_checks={"provider_attribution"},
        chain="arbitrum",
        expected_price_subjects={"WETH"},
    )

    assert derived["provider_attribution"]["status"] == "UNMEASURED"


def test_stale_provider_observation_cannot_be_freshened_by_a_false_boolean(qa, tmp_path: Path) -> None:
    _write_identity_records(tmp_path, [_identity()])

    derived = qa._derive_data_check_statuses(
        _provider_summary(observed_at="2026-08-14T12:00:00Z"),
        bundle=tmp_path,
        expected_checks={"provider_attribution"},
        chain="arbitrum",
        expected_price_subjects={"WETH"},
    )

    assert derived["provider_attribution"]["status"] == "UNMEASURED"


def test_provider_baseline_is_measured_but_legacy_identity_is_never_authoritative(qa, tmp_path: Path) -> None:
    _write_identity_records(tmp_path, [_identity()])

    derived = qa._derive_data_check_statuses(
        _provider_summary(),
        bundle=tmp_path,
        expected_checks={"provider_attribution"},
        chain="arbitrum",
        expected_price_subjects={"WETH"},
    )

    assert derived["provider_attribution"]["status"] == "DEGRADED"
    assert derived["provider_attribution"]["reason_codes"] == ["independent_authority_observation_absent"]


def test_artifact_generation_requires_the_exact_png_set_and_png_bytes(qa, tmp_path: Path) -> None:
    artifacts = {name: name for name in qa.DATA_CONTRACT_ARTIFACTS}
    for name in artifacts:
        (tmp_path / name).write_bytes(b"not a PNG artifact")
    summary = {
        "metrics": {"artifacts": artifacts},
        "checks": {"artifact_generation": {"ok": True}},
        "errors": [],
    }

    invalid = qa._derive_data_check_statuses(
        summary,
        bundle=tmp_path,
        expected_checks={"artifact_generation"},
        chain="arbitrum",
    )
    assert invalid["artifact_generation"]["status"] == "UNMEASURED"

    for name in artifacts:
        (tmp_path / name).write_bytes(b"\x89PNG\r\n\x1a\n" + b"measured")
    valid = qa._derive_data_check_statuses(
        summary,
        bundle=tmp_path,
        expected_checks={"artifact_generation"},
        chain="arbitrum",
    )
    assert valid["artifact_generation"]["status"] == "PASS"


def test_invalid_bundle_timestamp_cannot_produce_pass(qa, tmp_path: Path) -> None:
    catalog = {
        "chains": ["arbitrum"],
        "cells": [{"chain": "arbitrum", "check": "price_sanity"}],
    }
    summary = {
        "status": "pass",
        "timestamp_utc": "definitely-not-a-timestamp",
        "chain": "arbitrum",
        "checks": {"price_sanity": {"ok": True}},
        "errors": [],
        "metrics": {"prices": {"WETH": "3000"}},
    }

    with pytest.raises(ValueError, match="invalid timestamp_utc"):
        qa._validated_data_summary(summary, catalog=catalog, bundle=tmp_path)


def test_unmeasured_report_badges_are_not_green(qa) -> None:
    report = qa._render_data_report(
        {
            "status": "unmeasured",
            "chain": "arbitrum",
            "checks": {
                "provider_attribution": {
                    "status": "UNMEASURED",
                    "measurement": None,
                    "reason_codes": ["authoritative_resource_identity_absent"],
                }
            },
            "metrics": {},
        },
        "adversarial-unmeasured",
        git_sha="a" * 40,
        sealed_at="2026-08-15T12:00:00Z",
        artifact_paths=[],
    )

    assert '<span class="badge pass">Contract: UNMEASURED</span>' not in report
    assert '<span class="badge pass">Source health: UNMEASURED</span>' not in report
