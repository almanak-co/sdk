"""V3 core comparisons cover boundary equality, wrapping and uint128 fee casts."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.uniswap_v3.fee_growth import accrued, inside, sampled_inside

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-fee-vectors.json.gz").read_bytes()
    )
)


@pytest.mark.parametrize("method", ["inside", "earned"])
def test_fee_accounting_matches_compiled_v3_core(method):
    for row in CAPTURE["records"]:
        if row["method"] != method:
            continue
        result = inside(*row["args"]) if method == "inside" else (accrued(*row["args"]),)
        assert result == tuple(row["result"])


def test_fee_growth_refuses_invalid_units_and_uninitialized_historical_range():
    from types import SimpleNamespace

    with pytest.raises(ValueError, match="ordered ticks"):
        inside(0, 1, 0, (1, 2), (0, 0), (0, 0))
    with pytest.raises(ValueError, match="uint256"):
        inside(0, -1, 1, (-1, 2), (0, 0), (0, 0))
    with pytest.raises(ValueError, match="uint128"):
        accrued(2**128, 0, 1)
    with pytest.raises(ValueError, match="uint256"):
        accrued(1, -1, 1)
    target = SimpleNamespace(pool="explicit-pool", range=SimpleNamespace(lower=-1, upper=1))
    sample = SimpleNamespace(pool=SimpleNamespace(source=target.pool), ticks=())
    with pytest.raises(ValueError, match="initialized boundary"):
        sampled_inside(sample, target)
