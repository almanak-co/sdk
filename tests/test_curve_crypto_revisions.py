"""Compiled official revisions match pinned code, including constructor bindings."""

import gzip
import json

import pytest

from almanak.connectors.evm_revision import verify_code
from almanak.gateway.ethereum import DataError
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/curve-crypto-discovery")


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".gz")).read_bytes()))


@pytest.mark.parametrize("family,index", [("twocrypto", 0), ("tricrypto", 1)])
def test_pool_and_immutable_math_match_exact_compiled_revision(family, index):
    source = captured("curve-crypto-dependencies.json")
    pool = source["pools"][index]
    compiled = captured("curve-crypto-revisions.json")[family]
    for kind, address in (
        ("pool", pool["pool"]),
        ("math", pool["MATH()"]),
        ("factory", pool["factory"]),
        ("views", pool["views"]),
    ):
        raw = next(
            record["result"]
            for record in source["records"]
            if record["method"] == "eth_getCode" and record["params"][0] == address
        )
        values = verify_code(raw, compiled[kind])
        if kind == "pool":
            assert values["MATH"] == int(pool["MATH()"], 16)
            encoded = values["coins"].to_bytes(32 * len(pool["coins"]), "big")
            assert tuple(
                int.from_bytes(encoded[offset : offset + 32])
                for offset in range(0, len(encoded), 32)
            ) == tuple(int(coin, 16) for coin in pool["coins"])
        damaged = "0x00" + raw[4:]
        with pytest.raises(DataError, match="compiled"):
            verify_code(damaged, compiled[kind])


def test_twocrypto_original_blueprint_matches_compiled_creation():
    pool = captured("curve-crypto-dependencies.json")["pools"][0]
    revision = captured("curve-crypto-revisions.json")["twocrypto"]
    assert verify_code(pool["blueprint_code"], revision["blueprint"]) == {}
