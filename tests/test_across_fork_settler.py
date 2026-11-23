"""The fork settler's calls, offline: what it sends, in which order, and what it refuses.

The two-fork run against the real SpokePools is `tests/fork/test_across_settlement.py`.
"""

from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak import testflow_keepers
from almanak.connectors.across import fork_settler
from almanak.connectors.across.fork_settler import (
    FORK_CONSTRUCTED,
    LEAF,
    RELAY,
    RELAYED_ROOT_TOPIC,
    RELAYER,
    Settler,
    admin,
    refund_calls,
)
from almanak.connectors.across.operations import word
from almanak.connectors.across.settlement import ExternalEffects
from almanak.gateway.ethereum import DataError
from tests.across_support import ARBITRUM, BASE, NOW, RECIPIENT, SENDER
from tests.test_across_records import FORK_ARB, FORK_BASE, mined_deposit
from tests.test_across_records import config as config  # noqa: F401 - a fixture
from tests.test_across_settlement import FILL_DEADLINE

L1_ADMIN = "0x" + "c1" * 20


class Rpc:
    """A fork node that mines every transaction at once and keeps what was sent."""

    def __init__(self, now):
        self.now, self.sent, self.calls = now, [], []

    def __call__(self, method, params=()):
        self.calls.append(method)
        if method == "eth_getBlockByNumber":
            return {"timestamp": hex(self.now), "baseFeePerGas": "0x5"}
        if method == "evm_setNextBlockTimestamp":
            self.now = params[0]
        if method == "eth_call":
            return "0x" + word(L1_ADMIN).hex()
        if method == "eth_sendTransaction":
            self.sent.append(params[0])
            return hex(len(self.sent))
        if method == "eth_getTransactionReceipt":
            topics = [RELAYED_ROOT_TOPIC, "0x" + (7).to_bytes(32).hex()]
            return {
                "status": "0x1",
                "transactionHash": "0x" + params[0][2:].rjust(64, "0"),
                "blockNumber": "0x10",
                "logs": [{"topics": ["0x" + "00" * 32]}, {"topics": topics}],
            }
        return None


@pytest.fixture
def run(monkeypatch, config, tmp_path):  # noqa: F811
    outcome = mined_deposit(monkeypatch, config)
    nodes = {FORK_ARB.chain_id: Rpc(FILL_DEADLINE - 100), FORK_BASE.chain_id: Rpc(NOW)}
    funded = []
    monkeypatch.setattr(
        "almanak.gateway.fork_funding.fund_erc20",
        lambda node, fund, asset, amount: funded.append((fund, amount)) or {"method": "test"},
    )
    forks = SimpleNamespace(
        rpc=nodes.__getitem__, origin=lambda chain: SimpleNamespace(chain=chain)
    )
    forks.origin = lambda chain: SimpleNamespace(
        chain=ARBITRUM if chain == FORK_ARB.chain_id else BASE
    )
    state = SimpleNamespace(outcomes=(outcome,), transfers=(outcome.transfer,))
    settler = Settler(forks, tmp_path / "keepers")
    return SimpleNamespace(state=state, nodes=nodes, funded=funded, settler=settler)


def test_a_fill_is_the_pools_own_call_by_a_relayer_funded_with_the_offer(run, tmp_path):
    transfer = run.state.transfers[0]
    (row,) = run.settler.settle(run.state)
    approve, fill = run.nodes[FORK_BASE.chain_id].sent
    assert run.funded == [(RELAYER, transfer.offered.amount)]
    assert (approve["from"], fill["from"]) == (RELAYER, RELAYER)
    relay, repayment, payee = decode(
        (RELAY, "uint256", "bytes32"), bytes.fromhex(fill["data"][10:])
    )
    assert fill["data"][:10] == "0x" + keccak(text=f"fillRelay({RELAY},uint256,bytes32)")[:4].hex()
    # A fork relay names the isolated signing chain as its origin and is repaid there.
    assert (relay[0], relay[1], relay[7]) == (word(SENDER), word(RECIPIENT), 31337)
    assert (repayment, payee) == (31337, word(RELAYER))
    # The quote names an exclusive relayer: the fixture waits out its exclusivity.
    assert run.nodes[FORK_BASE.chain_id].now == relay[10] + 1
    assert (row["kind"], row["transfer"], row["advanced_seconds"] > 0) == (
        "fill",
        transfer.reference,
        True,
    )
    assert len(list((tmp_path / "keepers").iterdir())) == 1
    assert run.settler.align(run.state, 30) == 30 and run.nodes[FORK_ARB.chain_id].sent == []


def test_a_refund_is_a_labelled_fork_constructed_bundle_past_the_deadline(run):
    transfer = run.state.transfers[0]
    row = run.settler.refund(run.state, transfer)
    relayed, executed = run.nodes[FORK_ARB.chain_id].sent
    aliased = (int(L1_ADMIN, 16) + 0x1111000000000000000000000000000000001111) % 2**160
    assert int(relayed["from"], 16) == aliased and executed["from"] == RELAYER
    assert run.nodes[FORK_ARB.chain_id].now == FILL_DEADLINE + 1
    bundle, leaf, proof = decode(
        ("uint32", LEAF, "bytes32[]"), bytes.fromhex(executed["data"][10:])
    )
    assert (bundle, proof, leaf[1], leaf[2]) == (7, (), 31337, (transfer.sent.amount,))
    assert leaf[5][0].lower() == SENDER
    # The relayed root is the hash of that one leaf: an empty proof verifies it.
    assert bytes.fromhex(relayed["data"][10:74]) == keccak(encode((LEAF,), (leaf,)))
    assert (row["kind"], row["evidence"], row["root_bundle"]) == ("refund", FORK_CONSTRUCTED, 7)
    assert "no dataworker" in FORK_CONSTRUCTED and run.nodes[FORK_BASE.chain_id].sent == []
    # The destination fork is moved past the deadline too, so its scan can prove the
    # transfer unfilled: the refund is read only after that.
    destination = run.nodes[FORK_BASE.chain_id]
    assert destination.now == FILL_DEADLINE + 1 and "evm_mine" in destination.calls


def test_the_settler_refuses_what_it_cannot_settle(run, config, monkeypatch):  # noqa: F811
    with pytest.raises(DataError, match="no fork refund bundle is defined"):
        admin(run.nodes[FORK_BASE.chain_id], BASE.chain_id, "0x" + "09" * 20)
    stranger = run.state.transfers[0].model_copy(update={"reference": "1:0x" + "00" * 32 + ":1"})
    with pytest.raises(DataError, match="no retained deposit"):
        run.settler.fill(run.state, stranger)
    with pytest.raises(DataError, match="only on identified forks"):
        run.settler.fork(ARBITRUM)
    reverted = {"status": "0x0"}
    monkeypatch.setattr(fork_settler, "RECEIPT_SECONDS", 0.01)
    with pytest.raises(DataError, match="reverted"):
        fork_settler.receipt(lambda method, params: reverted, "0x01")
    with pytest.raises(DataError, match="remains unknown"):
        fork_settler.receipt(lambda method, params: None, "0x01")
    assert (
        refund_calls(1, SENDER, SENDER, 5)[0][:4]
        == keccak(text="relayRootBundle(bytes32,bytes32)")[:4]
    )


def test_the_fork_harness_takes_a_keeper_from_external_effects(monkeypatch, tmp_path):
    effects = ExternalEffects()
    connector = SimpleNamespace(requests=None, external_effects=effects)
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire", lambda config: connector
    )
    monkeypatch.setattr(testflow_keepers, "Forks", lambda root, env: SimpleNamespace())
    keepers = testflow_keepers.Keepers(tmp_path, tmp_path / ".env")
    open_state = SimpleNamespace(transfers=(SimpleNamespace(open=True),))
    monkeypatch.setattr(keepers, "decoded", lambda wire: open_state)
    binding, state = keepers.binding({"config": {}})
    assert binding is effects and isinstance(keepers.keeper, Settler)
    assert binding.unresolved(state) and keepers.step({"config": {}}, 12) == 12
    assert not effects.unresolved(SimpleNamespace(transfers=(SimpleNamespace(open=False),)))
    connector.external_effects = None
    assert keepers.binding({"config": {}}) == (None, None)
