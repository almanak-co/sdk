"""RPC witness discrimination with ABI bytes; not a live-fork acceptance."""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from qa_lab.chains import TOKENS
from qa_lab.e2e_pool import observe_pool
from qa_lab.e2e_stimulus import quote_stimulus


@pytest.fixture
def fork():
    weth = TOKENS["arbitrum"]["WETH"][0]
    usdc = TOKENS["arbitrum"]["USDC"][0]
    responses = {
        "0x0dfe1681": encode(["address"], [weth]),
        "0xd21220a7": encode(["address"], [usdc]),
        "0xddca3f43": encode(["uint24"], [500]),
        "0x3850c7bd": encode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            [2**96, 0, 0, 1, 1, 0, True],
        ),
    }
    reads = []
    blocks = []

    def call(tx, block_identifier):
        reads.append((tx, block_identifier))
        if tx["data"] == "0x313ce567":
            return encode(["uint8"], [18 if tx["to"].lower() == weth.lower() else 6])
        return responses[tx["data"]]

    block = {"number": 101, "hash": b"a" * 32, "timestamp": 1780000000}

    def get_block(identifier):
        blocks.append(identifier)
        return dict(block)

    web3 = SimpleNamespace(eth=SimpleNamespace(call=call, get_block=get_block))
    identities = []

    def identity(client=None):
        identities.append(client)
        return web3

    context = SimpleNamespace(
        chain="arbitrum",
        network="anvil",
        fork_block=100,
        assert_rpc_identity=identity,
        public_identity=lambda: {"instance_id": "owned-fixture"},
    )
    return SimpleNamespace(
        context=context, responses=responses, reads=reads, blocks=blocks, identities=identities, block=block, web3=web3
    )


def test_pool_witness_pins_all_reads_and_keeps_consumption_unknown(fork):
    result = observe_pool(fork.context)
    assert len(fork.reads) == 6
    assert all(number == 101 for _, number in fork.reads)
    assert fork.blocks == ["latest", 101]
    assert fork.identities == [None, fork.web3]
    assert Decimal(result["weth_usdc"]) == Decimal(10**12)
    assert len(result["raw_reads"]) == 6
    assert result["pool_quote_status"] == "MEASURED"
    assert result["consumed_input_status"] == "UNMEASURED"
    assert result["usd_anchor"] == "USDC_USD_1_ASSUMPTION"


@pytest.mark.parametrize(
    "selector,raw,reason",
    [
        ("0x3850c7bd", b"", "malformed"),
        ("0x3850c7bd", encode(["uint256"] * 7, [0, 0, 0, 1, 1, 0, 1]), "absent"),
        ("0xddca3f43", encode(["uint24"], [3000]), "expected"),
        ("0x0dfe1681", encode(["address"], [TOKENS["arbitrum"]["USDC"][0]]), "expected"),
    ],
)
def test_missing_or_contradictory_pool_bytes_refuse(fork, selector, raw, reason):
    fork.responses[selector] = raw
    with pytest.raises(ValueError, match=reason):
        observe_pool(fork.context)


def test_block_change_during_capture_refuses(fork):
    original = fork.web3.eth.call

    def reorg(tx, block_identifier):
        result = original(tx, block_identifier)
        fork.block["hash"] = b"b" * 32
        return result

    fork.web3.eth.call = reorg
    with pytest.raises(ValueError, match="block changed"):
        observe_pool(fork.context)


@pytest.mark.parametrize("field,value", [("chain", "base"), ("network", "mainnet")])
def test_other_execution_surface_refuses_before_any_rpc(fork, field, value):
    setattr(fork.context, field, value)
    with pytest.raises(ValueError, match="Arbitrum Anvil"):
        observe_pool(fork.context)
    assert fork.identities == []


def test_lost_fork_identity_refuses(fork):
    def lost(client=None):
        if client is not None:
            raise ValueError("Anvil instance changed")
        return fork.web3

    fork.context.assert_rpc_identity = lost
    with pytest.raises(ValueError, match="instance changed"):
        observe_pool(fork.context)


def _install_quoter(fork, *, malformed=False, reorg=False):
    pool_call = fork.web3.eth.call
    calls = []

    def call(tx, block_identifier):
        if len(tx["data"]) != 330:
            return pool_call(tx, block_identifier)
        token_in, token_out, amount, fee, limit = decode(
            ["address", "address", "uint256", "uint24", "uint160"], bytes.fromhex(tx["data"][10:])
        )
        assert token_in.lower() == TOKENS["arbitrum"]["USDC"][0].lower()
        assert token_out.lower() == TOKENS["arbitrum"]["WETH"][0].lower()
        assert fee == 500 and limit == 0
        assert block_identifier == 101
        calls.append(amount)
        if reorg:
            fork.block["hash"] = b"c" * 32
        if malformed:
            return b""
        return encode(
            ["uint256", "uint160", "uint32", "uint256"], [amount * 10, 2**96 * (10000 + amount) // 10000, 2, 50000]
        )

    fork.web3.eth.call = call
    return calls


def test_stimulus_quote_search_reaches_band_without_sending_transactions(fork):
    calls = _install_quoter(fork)
    result = quote_stimulus(
        fork.context, target_min=Decimal("1.07e12"), target_max=Decimal("1.08e12"), max_usdc_raw=1000
    )
    assert result["status"] == "QUOTED"
    assert result["execution_status"] == "UNMEASURED"
    selected = result["selected"]
    assert Decimal("1.07e12") <= Decimal(selected["weth_usdc_after"]) <= Decimal("1.08e12")
    assert 0 < int(selected["amount_in_raw"]) <= 1000
    assert len(calls) == len(result["quotes"]) <= 42
    assert len(calls) > 1


def test_stimulus_cap_cannot_be_widened_to_reach_target(fork):
    calls = _install_quoter(fork)
    result = quote_stimulus(fork.context, target_min=Decimal("1.07e12"), target_max=Decimal("1.08e12"), max_usdc_raw=10)
    assert result["status"] == "UNREACHABLE"
    assert result["selected"] is None
    assert calls == [10]


@pytest.mark.parametrize("failure", ["malformed", "reorg"])
def test_stimulus_requires_complete_unchanged_quote_evidence(fork, failure):
    _install_quoter(fork, **{failure: True})
    with pytest.raises(ValueError, match="malformed|block changed"):
        quote_stimulus(fork.context, target_min=Decimal("1.07e12"), target_max=Decimal("1.08e12"), max_usdc_raw=1000)


@pytest.mark.parametrize("cap", [0, -1, True, 10**13 + 1])
def test_stimulus_invalid_cap_refuses_before_rpc(fork, cap):
    with pytest.raises(ValueError, match="funding cap"):
        quote_stimulus(fork.context, target_min=Decimal(1), target_max=Decimal(2), max_usdc_raw=cap)
    assert fork.identities == []


def test_large_ceiling_does_not_force_a_full_ceiling_probe(fork):
    calls = _install_quoter(fork)
    result = quote_stimulus(
        fork.context, target_min=Decimal("1.07e12"), target_max=Decimal("1.08e12"), max_usdc_raw=10**13
    )
    assert result["status"] == "QUOTED"
    assert calls[0] == 10**12
    assert max(calls) == 10**12
    assert len(calls) <= (10**13).bit_length() + 1


def test_quote_timeout_retains_requested_input_without_a_completed_result(fork, tmp_path):
    original = fork.web3.eth.call
    trace = tmp_path / "quotes.jsonl"

    def timeout(tx, block_identifier):
        if len(tx["data"]) == 330:
            events = [json.loads(line) for line in trace.read_text().splitlines()]
            assert events[-1]["stage"] == "request"
            assert events[-1]["calldata"] == tx["data"]
            raise TimeoutError("probe timeout")
        return original(tx, block_identifier)

    fork.web3.eth.call = timeout
    with pytest.raises(TimeoutError):
        quote_stimulus(
            fork.context,
            target_min=Decimal("1.07e12"),
            target_max=Decimal("1.08e12"),
            max_usdc_raw=1000,
            trace_path=trace,
        )
    events = [json.loads(line) for line in trace.read_text().splitlines()]
    assert [event["stage"] for event in events] == ["started", "pool", "request", "failed"]
    assert events[-1]["error_type"] == "TimeoutError"
    assert [event["sequence"] for event in events] == [0, 1, 2, 3]
    with pytest.raises(FileExistsError):
        quote_stimulus(
            fork.context,
            target_min=Decimal("1.07e12"),
            target_max=Decimal("1.08e12"),
            max_usdc_raw=1000,
            trace_path=trace,
        )
