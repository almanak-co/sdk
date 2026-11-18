"""Apply shared family integrity and cleanup checks to BNB archive datasets."""

import gzip
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.connectors.curve.economic_binding import CurveEconomic
from tests import test_curve_crypto_prepared_runner as two
from tests import test_curve_prepared_runner as stable
from tests.curve_retired import current_json

DIRECTORY = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-07-rollout"


@pytest.fixture(params=(("stable", stable), ("twocrypto", two)))
def captured(request, monkeypatch):
    family, module = request.param

    def raw(kind, name):
        return gzip.decompress(
            (DIRECTORY / f"curve-bnb-{family}-prepared" / "lp" / (name + ".gz")).read_bytes()
        )

    monkeypatch.setattr(module, "raw", raw)
    manifest = CurveEconomic.manifest_type.model_validate_json(
        current_json(raw("lp", "run/manifest.json"))
    )
    assert all(a.chain.chain_id == ChainId("eip155:56") for a in manifest.config.allocations)
    return module, ("lp", manifest)


def test_archive_payload_and_sample_time_tampering_refused(captured, tmp_path):
    module, inputs = captured
    module.test_captured_pool_book_rejects_changed_payload_and_future_sample(inputs, tmp_path)


def test_captured_history_rebuilds_the_same_dataset(captured, tmp_path):
    module, inputs = captured
    module.test_replacement_history_gateway_reproduces_complete_dataset(inputs, tmp_path)


def test_cleanup_preserves_the_captured_fund_inventory(captured, tmp_path):
    module, inputs = captured
    module.test_revoke_preserves_captured_inventory(inputs, tmp_path)
