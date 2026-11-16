"""Canonical Robinhood costs and explicit Nitro legacy-price bounds."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.gateway.production_fees import validate_receipt_cost
from almanak.models import AssetAmount, Block, Chain

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-fees"
)
CAPTURE = json.loads(gzip.decompress((DIRECTORY / "proof.json.gz").read_bytes()))


def attempt(case, chain="eip155:4663"):
    scope = Chain(chain_id=ChainId(chain), environment="mainnet")
    tx = case["transaction"]
    return SimpleNamespace(
        gas=int(tx["gas"], 16),
        gas_price=int(tx["gasPrice"], 16),
        operation=SimpleNamespace(target=SimpleNamespace(fund=SimpleNamespace(chain=scope))),
        before=SimpleNamespace(
            native_balance=AssetAmount(
                asset=AssetType(f"{chain}/slip44:60"),
                amount=int(case["balance_before"], 16),
            )
        ),
    )


def captured_block():
    block = CAPTURE["block"]
    return Block(
        chain=Chain(chain_id=ChainId("eip155:4663"), environment="mainnet"),
        number=int(block["number"], 16),
        hash=block["hash"],
        timestamp=int(block["timestamp"], 16),
    )


@pytest.fixture
def legacy():
    case = next(c for c in CAPTURE["cases"] if c["transaction"]["type"] == "0x0")
    return SimpleNamespace(production=True), attempt(case), case["receipt"]


@pytest.mark.parametrize("chain", ["eip155:4663", "eip155:42161"])
def test_nitro_profiles_accept_measured_discount_without_changing_reserved_bid(legacy, chain):
    config, pending, receipt = legacy
    pending.operation.target.fund.chain = Chain(chain_id=ChainId(chain), environment="mainnet")
    bid = pending.gas_price
    assert int(receipt["effectiveGasPrice"], 16) < bid
    validate_receipt_cost(config, pending, receipt)
    assert pending.gas_price == bid


@pytest.mark.parametrize("chain", ["eip155:1", "eip155:10", "eip155:8453", "eip155:56"])
def test_other_profiles_keep_exact_legacy_bid_validation(legacy, chain):
    config, pending, receipt = legacy
    pending.operation.target.fund.chain = Chain(chain_id=ChainId(chain), environment="mainnet")
    with pytest.raises(DataError, match="admitted legacy"):
        validate_receipt_cost(config, pending, receipt)
    validate_receipt_cost(config, pending, {**receipt, "effectiveGasPrice": hex(pending.gas_price)})


@pytest.mark.parametrize("chain", ["eip155:4663", "eip155:42161"])
@pytest.mark.parametrize(
    "change", ["price_above_bid", "negative_price", "gas_above_limit", "zero_gas"]
)
def test_nitro_profile_cannot_exceed_admitted_fee_bounds(legacy, chain, change):
    config, pending, receipt = legacy
    pending.operation.target.fund.chain = Chain(chain_id=ChainId(chain), environment="mainnet")
    receipt = dict(receipt)
    field, value = {
        "price_above_bid": ("effectiveGasPrice", pending.gas_price + 1),
        "negative_price": ("effectiveGasPrice", -1),
        "gas_above_limit": ("gasUsed", pending.gas + 1),
        "zero_gas": ("gasUsed", 0),
    }[change]
    receipt[field] = hex(value)
    with pytest.raises(DataError, match="admitted legacy"):
        validate_receipt_cost(config, pending, receipt)


def test_legacy_fork_transport_uses_the_verified_origin_fee_profile(legacy):
    _, pending, receipt = legacy
    instance = "0x" + "11" * 32
    fork_chain = Chain(chain_id=ChainId("eip155:31337"), environment="anvil:" + instance)
    binding = ForkBinding(chain=fork_chain, origin=captured_block(), instance=instance)
    config = SimpleNamespace(production=None, fork_bindings=(binding,))
    pending.operation.target.fund.chain = fork_chain
    validate_receipt_cost(config, pending, receipt)


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_actual_mined_total_includes_l1_once_for_both_transaction_types(case):
    pending = attempt(case)
    backend = SimpleNamespace(config=SimpleNamespace(production=True))
    receipt = case["receipt"]
    cost, evidence = charged_fees(backend, pending, receipt, captured_block())
    assert int(receipt["gasUsedForL1"], 16) > 0
    assert cost.amount == int(case["balance_before"], 16) - int(case["balance_after"], 16)
    assert cost.amount == int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16)
    assert cost.asset == pending.before.native_balance.asset
    assert evidence is None
