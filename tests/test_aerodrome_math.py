import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.aerodrome.math import accrued, quote

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/aerodrome-base-discovery"
)


def retained(name):
    return json.loads(gzip.decompress((EVIDENCE / name).read_bytes()))


def test_integer_outputs_and_reverts_match_compiled_verified_pool():
    records = retained("math-vectors.json.gz")["records"]
    assert len(records) == 1210
    assert sum(record["reverted"] for record in records) == 5
    for record in records:
        if record["reverted"]:
            with pytest.raises(ValueError):
                quote(*record["args"])
        else:
            assert quote(*record["args"]) == record["result"]


def test_pinned_stable_and_volatile_pool_quotes():
    rows = retained("discovery.json.gz")["rows"]
    assert {row["stable"] for row in rows} == {False, True}
    for row in rows:
        d0, d1, r0, r1, *_ = row["metadata"]
        for result in row["quotes"]:
            direction = result["asset"] == row["tokens"][0]["identifier"]
            assert (
                quote(row["stable"], d0, d1, r0, r1, result["amount"], row["fee_bps"], direction)
                == result["output"]
            )


def test_fees_do_not_wrap_and_remain_claimable_after_share_exit():
    assert accrued(0, 10**18, 0, 123) == 123
    with pytest.raises(ValueError, match="overflow or underflow"):
        accrued(1, 0, 1, 0)
    with pytest.raises(ValueError, match="overflow or underflow"):
        accrued(2**256 - 1, 2, 0, 0)


def test_fees_match_actual_fork_claims_for_both_invariants():
    for row in retained("operations.json.gz"):
        claims = [step for step in row["steps"] if step["kind"] == "collect"]
        assert len(claims) == 3
        for step in claims:
            before, after = step["before"], step["after"]
            for index in (0, 1):
                expected = accrued(
                    before["shares"],
                    before["indices"][index],
                    before["supply_indices"][index],
                    before["claimable"][index],
                )
                assert after["wallets"][index] - before["wallets"][index] == expected
