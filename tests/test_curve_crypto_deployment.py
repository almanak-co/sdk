"""Captured RPC evidence verifies configured Crypto contracts without provider access."""

import json

import pytest

from almanak.caip import ChainId
from almanak.connectors.curve.crypto_deployment import verify_pool
from almanak.connectors.curve.domain import CurvePool
from almanak.gateway.ethereum import DataError
from almanak.models import Chain
from tests.test_curve_crypto_revisions import captured
from tests.test_curve_reader import CapturedReader, request_key


@pytest.fixture
def deployment():
    data = captured("curve-crypto-dependencies.json")
    pool = data["pools"][0]
    source = CurvePool.model_validate_json(
        json.dumps(
            {
                "family": "twocrypto_ng",
                "chain": data["block"]["chain"],
                "factory": pool["factory"],
                "pool": pool["pool"],
                "coins": ["eip155:1/erc20:" + address for address in pool["coins"]],
            }
        )
    )
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["block"]["number"])
        yield source, reader, block, pool


def test_configured_two_coin_deployment_retains_original_blueprint_and_bound_math(deployment):
    source, reader, block, pool = deployment
    result = verify_pool(reader, source, block)
    assert result.precisions == tuple(pool["precisions()"])
    assert result.blueprint.address == pool["retained_blueprint"]
    assert result.math.address == pool["MATH()"]
    assert result.views.address == pool["views"]
    assert len(result.identities) == 5


def test_reordered_coins_or_other_chain_cannot_reuse_the_same_pool(deployment):
    source, reader, block, _ = deployment
    with pytest.raises(DataError, match="coin order"):
        verify_pool(reader, source.model_copy(update={"coins": source.coins[::-1]}), block)
    other_chain = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
    with pytest.raises(DataError, match="execution scope"):
        verify_pool(reader, source.model_copy(update={"chain": other_chain}), block)


@pytest.mark.parametrize("role", ["factory", "pool", "MATH()", "views", "retained_blueprint"])
def test_changed_code_cannot_reuse_revision_or_permission_identity(deployment, role):
    source, reader, block, pool = deployment
    key = request_key(
        "eth_getCode", [pool[role], {"blockHash": block.hash, "requireCanonical": True}]
    )
    original = reader.records[key]
    first = int(original[2:4], 16) ^ 1
    reader.records[key] = "0x" + f"{first:02x}" + original[4:]
    with pytest.raises(DataError, match="compiled"):
        verify_pool(reader, source, block)
