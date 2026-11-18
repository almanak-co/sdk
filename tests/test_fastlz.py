"""Compare fee compression lengths with independently compiled FastLZ level 1."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.gateway.fastlz import compressed_size

VECTORS = json.loads(
    gzip.decompress((Path(__file__).parent / "fixtures/fastlz-level1-vectors.json.gz").read_bytes())
)["cases"]


@pytest.mark.parametrize(
    "case", VECTORS, ids=lambda case: f"{case['label']}-{len(case['data']) // 2}"
)
def test_compressed_length_matches_independent_c_encoder(case):
    assert compressed_size(bytes.fromhex(case["data"])) == case["size"]


@pytest.mark.parametrize("length", range(16))
def test_short_inputs_are_literal_runs(length):
    assert compressed_size(bytes(range(length))) == length + bool(length)
