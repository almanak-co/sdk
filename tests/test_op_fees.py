"""Retained Base fork payer debits and pinned fee-parameter reads."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm_fees import OpFeeEvidence, charged_fees, validate_fee_evidence
from almanak.gateway.op_fees import RLP_OVERHEAD, FjordParameters, exposure_bound, read_parameters
from almanak.models import AssetAmount, Block


def captured():
    path = Path(__file__).parent / "fixtures/base-fjord-fee-reads.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def replay(raw, chain):
    rows = {
        json.dumps([r["method"], r["params"]], sort_keys=True): r["result"] for r in raw["reads"]
    }

    def respond(request):
        body = json.loads(request.content)
        result = rows[json.dumps([body["method"], body["params"]], sort_keys=True)]
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    client = httpx.Client(transport=httpx.MockTransport(respond))
    return client, EthereumReader(chain, "https://archive.invalid", client=client)


@pytest.mark.parametrize("index", range(5))
def test_pinned_fjord_fees_match_actual_anvil_payer_debits(index):
    raw = captured()
    case = raw["cases"][index]
    expected = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    client, reader = replay(raw, expected.block.chain)
    with client, reader:
        parameters = read_parameters(reader, expected.block)
    assert parameters == expected
    receipt = case["receipt"]
    gas = int(receipt["gasUsed"], 16)
    l1_fee = parameters.l1_fee(bytes.fromhex(case["raw_transaction"][2:]))
    assert l1_fee > 0
    assert "l1Fee" not in receipt
    actual = gas * int(receipt["effectiveGasPrice"], 16) + l1_fee + parameters.operator_fee(gas)
    assert actual == case["payer_before"] - case["payer_after"]


def test_unverified_fee_implementation_fails_closed():
    raw = captured()
    parameters = FjordParameters.model_validate_json(json.dumps(raw["cases"][0]["parameters"]))
    for row in raw["reads"]:
        if row["method"] == "eth_getCode":
            row["result"] = "0x00"
    client, reader = replay(raw, parameters.block.chain)
    with client, reader, pytest.raises(DataError, match="unverified OP fee"):
        read_parameters(reader, parameters.block)


def test_base_l1block_1_8_0_is_an_accepted_revision():
    """Base upgraded L1Block to 1.8.0 at block 53508490; these are its actual reads after it."""
    from almanak.caip import ChainId
    from almanak.gateway.op_fees import L1_BLOCK, verify_revision
    from almanak.models import Chain

    raw = json.loads(
        gzip.decompress(
            (Path(__file__).parent / "fixtures/base-l1block-1.8.0-reads.json.gz").read_bytes()
        )
    )
    base = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
    client, reader = replay(raw, base)
    block = SimpleNamespace(hash=raw["block"]["hash"])
    with client, reader:
        verify_revision(reader, block, L1_BLOCK)


@pytest.mark.parametrize(
    "revision,expected", [("fjord", 0), ("isthmus", 58), ("jovian", 5_100_000_007)]
)
def test_operator_revisions_preserve_their_distinct_units(revision, expected):
    values = captured()["cases"][0]["parameters"]
    values.update(revision=revision, operator_fee_scalar=17, operator_fee_constant=7)
    parameters = FjordParameters.model_validate_json(json.dumps(values))
    assert parameters.operator_fee(3_000_000) == expected
    with pytest.raises(ValueError, match="negative"):
        parameters.operator_fee(-1)


@pytest.mark.parametrize("mismatch", [False, True])
def test_shared_cost_keeps_chain_scope_and_visible_op_evidence(mismatch):
    raw = captured()
    case = raw["cases"][-1]
    parameters = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    origin = Block.model_validate_json(json.dumps(raw["origin"]))
    balance = AssetAmount.model_validate_json(
        json.dumps({"asset": str(parameters.block.chain.chain_id) + "/slip44:60", "amount": 1})
    )
    pending = SimpleNamespace(
        before=SimpleNamespace(native_balance=balance),
        operation=SimpleNamespace(
            target=SimpleNamespace(fund=SimpleNamespace(chain=parameters.block.chain))
        ),
        raw_transaction=case["raw_transaction"],
        tx_hash=case["receipt"]["transactionHash"],
    )
    config = SimpleNamespace(
        production=None,
        fork_bindings=(SimpleNamespace(chain=parameters.block.chain, origin=origin),),
    )
    receipt = case["receipt"]
    receipt["l1Fee"] = hex(parameters.l1_fee(bytes.fromhex(case["raw_transaction"][2:])) + mismatch)
    client, reader = replay(raw, parameters.block.chain)
    with client, reader:
        backend = SimpleNamespace(config=config, reader=reader)
        if mismatch:
            with pytest.raises(DataError, match="receipt L1 fee differs"):
                charged_fees(backend, pending, receipt, parameters.block)
            return
        cost, evidence = charged_fees(backend, pending, receipt, parameters.block)
    assert cost.asset == balance.asset
    assert cost.amount == case["payer_before"] - case["payer_after"]
    assert evidence.parameters == parameters
    assert evidence.transaction_hash == pending.tx_hash


@pytest.mark.parametrize("index", range(5))
def test_quote_time_exposure_bound_covers_the_mined_op_cost(index):
    raw = captured()
    case = raw["cases"][index]
    parameters = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    signed = bytes.fromhex(case["raw_transaction"][2:])
    gas_used = int(case["receipt"]["gasUsed"], 16)
    mined = parameters.l1_fee(signed) + parameters.operator_fee(gas_used)
    client, reader = replay(raw, parameters.block.chain)
    data_length = len(signed) - RLP_OVERHEAD if len(signed) > RLP_OVERHEAD else 0
    with client, reader:
        exact = exposure_bound(reader, parameters.block, gas_used, data_length, 10_000)
        doubled = exposure_bound(reader, parameters.block, gas_used, data_length, 20_000)
    assert mined <= exact < doubled
    with pytest.raises(ValueError, match="negative"):
        exposure_bound(reader, parameters.block, -1, data_length, 10_000)


def test_exposure_bound_grows_with_calldata_and_margins_only_the_variable_part():
    raw = captured()
    case = raw["cases"][0]
    parameters = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    client, reader = replay(raw, parameters.block.chain)
    with client, reader:
        small = exposure_bound(reader, parameters.block, 100_000, 4, 10_000)
        large = exposure_bound(reader, parameters.block, 100_000, 4_096, 10_000)
        margined = exposure_bound(reader, parameters.block, 100_000, 4, 15_000)
    assert small < large
    variable = small - parameters.operator_fee_constant
    assert margined == (variable * 15_000 + 9_999) // 10_000 + parameters.operator_fee_constant


@pytest.mark.parametrize("mutation", ["raw_transaction", "l1_fee_wei", "operator_fee_wei"])
def test_fee_evidence_rejects_changed_calculation_inputs(mutation):
    case = captured()["cases"][-1]
    parameters = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    gas_used = int(case["receipt"]["gasUsed"], 16)
    values = {
        "transaction_hash": case["receipt"]["transactionHash"],
        "raw_transaction": case["raw_transaction"],
        "parameters": parameters.model_dump(mode="json"),
        "gas_used": gas_used,
        "l1_fee_wei": parameters.l1_fee(bytes.fromhex(case["raw_transaction"][2:])),
        "operator_fee_wei": parameters.operator_fee(gas_used),
    }
    evidence = OpFeeEvidence.model_validate_json(json.dumps(values))
    cost = AssetAmount.model_validate_json(
        json.dumps(
            {
                "asset": str(parameters.block.chain.chain_id) + "/slip44:60",
                "amount": case["payer_before"] - case["payer_after"],
            }
        )
    )
    with pytest.raises(ValueError, match="payer cost differs"):
        validate_fee_evidence(
            evidence,
            evidence.transaction_hash,
            parameters.block,
            case["receipt"],
            cost.model_copy(update={"amount": cost.amount + 1}),
        )
    with pytest.raises(ValueError, match="receipt identity"):
        validate_fee_evidence(evidence, "0x" + "00" * 32, parameters.block, case["receipt"], cost)
    if mutation == "raw_transaction":
        values[mutation] = "0x00"
    else:
        values[mutation] += 1
    with pytest.raises(ValueError, match="OP fee"):
        OpFeeEvidence.model_validate_json(json.dumps(values))
