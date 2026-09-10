"""NFT disappearance, residual liquidity, and missing reads remain distinct."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from qa_lab.e2e_positions import POSITION_TYPES, TRANSFER, capture_position_generations

WALLET = "0x" + "11" * 20
OTHER = "0x" + "22" * 20
ZERO = "0x" + "00" * 20


def transfer(sender, recipient, index):
    return {
        "address": UNISWAP_V3["arbitrum"]["position_manager"],
        "blockNumber": 101,
        "blockHash": b"a" * 32,
        "logIndex": index,
        "transactionHash": b"b" * 32,
        "topics": [
            TRANSFER,
            bytes.fromhex(sender[2:]).rjust(32, b"\0"),
            bytes.fromhex(recipient[2:]).rjust(32, b"\0"),
            (7).to_bytes(32, "big"),
        ],
        "data": b"",
    }


@pytest.fixture
def fork():
    logs = [transfer(ZERO, WALLET, 0)]
    values = [0, ZERO, WALLET, OTHER, 500, -10, 10, 123, 0, 0, 0, 0]
    reads = []

    def call(tx, block_identifier):
        reads.append(block_identifier)
        return encode(POSITION_TYPES, values)

    client = SimpleNamespace(
        eth=SimpleNamespace(
            get_logs=lambda query: logs,
            get_block=lambda tag: {"number": 101, "hash": b"a" * 32},
            call=call,
        )
    )
    context = SimpleNamespace(
        chain="arbitrum",
        network="anvil",
        fork_block=100,
        assert_rpc_identity=lambda client_arg=None: client,
        public_identity=lambda: {"instance_id": "test-fork"},
    )
    return SimpleNamespace(context=context, logs=logs, values=values, reads=reads, client=client)


def test_residual_liquidity_is_retained_without_self_certifying_closure(fork):
    evidence = capture_position_generations(fork.context, WALLET, 101)
    assert evidence["generations"][0]["terminal_liquidity_raw"] == "123"
    assert evidence["closure_verdict"] == "UNMEASURED"
    assert fork.reads == [101]
    assert evidence["logs"][0]["topics"][3] == "0x" + f"{7:064x}"


def test_transferring_an_nft_away_does_not_make_it_closed(fork):
    fork.logs.append(transfer(WALLET, OTHER, 1))
    position = capture_position_generations(fork.context, WALLET, 101)["generations"][0]
    assert position["owner"] == OTHER
    assert position["burned"] is False
    assert position["terminal_liquidity_raw"] == "123"


def test_burn_evidence_is_distinct_from_a_measured_zero(fork):
    fork.logs.append(transfer(WALLET, ZERO, 1))
    position = capture_position_generations(fork.context, WALLET, 101)["generations"][0]
    assert position["terminal_status"] == "BURNED"
    assert position["terminal_liquidity_raw"] is None
    assert fork.reads == []


def test_missing_terminal_read_is_not_zero(fork):
    fork.client.eth.call = lambda *args, **kwargs: b""
    with pytest.raises(ValueError, match="unmeasured"):
        capture_position_generations(fork.context, WALLET, 101)


def test_imported_position_cannot_hide_outside_generation_census(fork):
    fork.logs[:] = [transfer(OTHER, WALLET, 0)]
    with pytest.raises(ValueError, match="without a witnessed"):
        capture_position_generations(fork.context, WALLET, 101)


@pytest.mark.parametrize("fault", ["duplicate", "removed", "unknown_sender"])
def test_contradictory_census_refuses(fork, fault):
    if fault == "duplicate":
        fork.logs.append(dict(fork.logs[0]))
    elif fault == "removed":
        fork.logs[0]["removed"] = True
    else:
        fork.logs.append(transfer(OTHER, ZERO, 1))
    with pytest.raises(ValueError):
        capture_position_generations(fork.context, WALLET, 101)


def test_shared_terminal_pin_survives_head_advancement(fork):
    def block(tag):
        if tag == "latest":
            return {"number": 102, "hash": b"c" * 32}
        assert tag == 101
        return {"number": 101, "hash": b"a" * 32}

    fork.client.eth.get_block = block
    raw = capture_position_generations(fork.context, WALLET, 101, terminal_block=(101, "0x" + "61" * 32))
    assert raw["end_block"] == 101
    assert fork.reads == [101]


def test_changed_shared_terminal_pin_is_rejected(fork):
    with pytest.raises(ValueError, match="shared pin"):
        capture_position_generations(fork.context, WALLET, 101, terminal_block=(101, "0x" + "cc" * 32))
