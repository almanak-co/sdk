"""`almanak ax recover` on a signed step whose nonce another transaction took.

Each rehearsal signs a step with the production adapter on an isolated Anvil fork,
keeps it from being mined, and mines another transaction of the same wallet at its
nonce. The record stays while that transaction is not final and is released, with the
transaction named, once it is behind the node's `finalized` block and behind the head
by the network's own margin. Every declared Curve cell is rehearsed, since a Curve
call carries no deadline that could retire it; one approval, one lending step and one
deadline-bound swap show the rule is not Curve's. A cell on a network that states no
margin keeps its record. The fork's `finalized` block is the node's own, 64 blocks
behind its head. It is rehearsal evidence, not a mainnet run.
"""

from contextlib import contextmanager
from pathlib import Path

import pytest

from almanak import ax_actions
from almanak import state as durable
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import Chain
from tests.fork import test_ax_pool_lending as lending
from tests.fork import test_ax_swap as v3
from tests.fork import test_ax_swap_curve as curve

pytestmark = pytest.mark.fork
RECEIPT = "eth_getTransactionReceipt"
# Blocks mined to put a transaction behind the node's finalized block, 64 behind its head.
FINALITY = 70
CLAIMS = "almanak.state.claims_root"


def margin_of(chain) -> int | None:
    return evm_network(ChainId(str(chain))).finality_margin_seconds


def cells(stated: bool):
    """The declared Curve cells on networks that state a finality margin, or on the rest."""
    return [cell for cell in curve.declared_cells() if (margin_of(cell[0]) is not None) is stated]


def named(cell) -> str:
    return f"{curve.NETWORKS[cell[0]]}-{cell[1]}"


@contextmanager
def forked(request, chain_id, height, block_hash):
    env = request.config.getoption("--fork-env").resolve()
    chain = Chain(chain_id=chain_id, environment="mainnet")
    with EthereumReader(chain, read_endpoint(chain_id, env)) as reader:
        origin = reader.pin_block(height)
    assert origin.hash == block_hash
    target = ForkTarget(chain=chain, provider=ax_actions.alchemy(chain_id), block=origin)
    with Fork(env, origin, target=target) as running:
        yield running


@pytest.fixture(params=cells(stated=True), ids=named)
def cell(request):
    return curve.reference(*request.param)


@pytest.fixture
def setting(request, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state.
    monkeypatch.setattr(CLAIMS, lambda: tmp_path / "state/claims")
    with forked(request, cell.chain, cell.height, cell.block_hash) as fork:
        wallet = curve.funded_wallet(fork, cell, tmp_path / "wallet")
        yield curve.Setting(fork, cell, wallet, curve.as_mainnet(fork, cell.chain))


@pytest.fixture
def v3_setting(request, tmp_path, monkeypatch):
    monkeypatch.setattr(CLAIMS, lambda: tmp_path / "state/claims")
    cell = v3.reference("uniswap_v3", "eip155:8453")
    with forked(request, cell.chain, cell.height, cell.block_hash) as fork:
        wallet = v3.funded_wallet(fork, cell, tmp_path / "wallet")
        yield v3.Setting(fork, cell, wallet, v3.as_mainnet(fork, cell.chain))


@pytest.fixture
def desk(request, tmp_path, monkeypatch):
    monkeypatch.setattr(CLAIMS, lambda: tmp_path / "state/claims")
    chain, anchor, second = lending.fixture_of("aave_v3_ethereum_lending")
    with forked(request, chain, anchor["height"], anchor["reference"]) as fork:
        yield lending.Desk(fork, "aave_v3", chain, second, tmp_path)


def signed_step(record: Path):
    return durable.Journal(record / "state.sqlite").load().pending


def take_nonce(fork, owner: str) -> str:
    """Mine another transaction of the wallet at its next nonce; returns its hash."""
    fork.rpc("anvil_impersonateAccount", [owner])
    price = fork.rpc("eth_gasPrice")
    sent = fork.rpc("eth_sendTransaction", [{"from": owner, "to": owner, "gasPrice": price}])
    fork.rpc.wait_receipt(sent)
    fork.rpc("anvil_stopImpersonatingAccount", [owner])
    return sent


def finalize(fork, seconds: int = FINALITY):
    """Mine past the node's finalized block, over at least this many seconds."""
    fork.rpc("anvil_mine", [hex(FINALITY), hex(seconds // FINALITY + 1)])


def assert_kept(refused, step, reason):
    """The step is neither sent nor dropped, and the refusal says why."""
    assert (refused["resolved"], refused["refused"]["kind"]) == (False, "nonce_conflict")
    assert reason in refused["refused"]["message"]
    assert refused["unresolved"]["transaction"] == step.tx_hash
    assert "released_consumed_nonce" not in refused


def assert_listed(step):
    (listed,) = ax_actions.kept_records()
    assert listed["unresolved"]["transaction"] == step.tx_hash


def assert_released(fork, result, step, consumer):
    """The result names the step, the transaction that took its nonce and its block."""
    mined = fork.rpc(RECEIPT, [consumer])
    held = {
        "transaction": consumer,
        "block": int(mined["blockNumber"], 16),
        "block_hash": mined["blockHash"],
    }
    assert result["released_consumed_nonce"] == [
        {
            "operation": step.operation.kind,
            "transaction": step.tx_hash,
            "nonce": step.nonce,
            "consumed_by": held,
        }
    ]
    assert (result["resolved"], result["discarded_request"]) == (True, False)
    assert not {"unresolved", "refused", "record_kept"} & set(result)
    assert ax_actions.kept_records() == []


def assert_dead(fork, step):
    """The node holds no copy of the signed bytes and refuses them for their nonce."""
    known = [fork.rpc(method, [step.tx_hash]) for method in ("eth_getTransactionByHash", RECEIPT)]
    assert known == [None, None]
    with pytest.raises(RuntimeError, match="nonce too low"):
        fork.rpc("eth_sendRawTransaction", [step.raw_transaction])


def released_once_final(fork, recover, record, owner):
    """Take the step's nonce, ask before and after that is final, and check both answers."""
    step = signed_step(record)
    consumer = take_nonce(fork, owner)
    assert_kept(recover(record), step, "not consumed at the finalized block")
    assert_listed(step)
    finalize(fork, margin_of(step.before.block.chain.chain_id))
    result = recover(record)
    assert_released(fork, result, step, consumer)
    assert_dead(fork, step)
    assert not record.exists()
    return result


def test_a_sent_curve_swap_whose_nonce_was_taken_is_released_once_that_is_final(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record, _ = curve.sent_but_never_mined(setting, monkeypatch, capsys)

    result = released_once_final(setting.fork, setting.recover, record, setting.owner)
    # Nothing was sold, and the approval the swap would have spent still stands.
    assert curve.operations(result) == ["approve"]
    assert (setting.balances(), setting.pool_allowance()) == (start, setting.cell.amount)

    # The wallet is free: the swap stated again needs only its own step.
    again = setting(execute=True)
    assert (curve.operations(again), again["completed"]) == (["swap"], True)
    assert start[0] - setting.balances()[0] == setting.cell.amount


ONE = [("eip155:1", "stableswap_ng")]


@pytest.mark.parametrize("cell", ONE, indirect=True, ids=["ethereum-stableswap_ng"])
def test_a_finalized_tag_alone_releases_nothing_within_the_networks_margin(
    setting, monkeypatch, capsys
):
    """Behind the node's finalized block, and still younger than Ethereum's own margin."""
    record, _ = curve.sent_but_never_mined(setting, monkeypatch, capsys)
    step = signed_step(record)
    consumer = take_nonce(setting.fork, setting.owner)
    finalize(setting.fork)
    assert_kept(setting.recover(record), step, "it counts as final 1152s behind it")
    assert_listed(step)

    finalize(setting.fork, margin_of(setting.cell.chain))
    assert_released(setting.fork, setting.recover(record), step, consumer)


@pytest.mark.parametrize("cell", cells(stated=False), indirect=True, ids=named)
def test_a_network_that_states_no_margin_keeps_the_record(setting, monkeypatch, capsys):
    record, _ = curve.sent_but_never_mined(setting, monkeypatch, capsys)
    step = signed_step(record)
    take_nonce(setting.fork, setting.owner)
    finalize(setting.fork, 3600)
    assert_kept(setting.recover(record), step, "states no finality margin")
    assert_listed(step)


@pytest.mark.parametrize("cell", ONE, indirect=True, ids=["ethereum-stableswap_ng"])
@pytest.mark.parametrize("kind", ["approve", "swap"])
def test_a_step_signed_and_never_sent_is_released_the_same_way(setting, kind, monkeypatch, capsys):
    start = setting.balances()
    record = curve.stopped_while_waiting_for(kind, setting, monkeypatch, capsys)
    step = signed_step(record)
    assert (step.operation.kind, curve.is_unsent(step)) == (kind, True)

    result = released_once_final(setting.fork, setting.recover, record, setting.owner)
    # Only what was mined before the step is among the outcomes, and nothing was sold.
    earlier = {"approve": [], "swap": ["approve"]}[kind]
    assert (curve.operations(result), setting.balances()) == (earlier, start)
    assert setting(execute=True)["completed"]


def test_a_swap_with_a_deadline_whose_nonce_was_taken_is_released_by_that_proof(
    v3_setting, monkeypatch, capsys
):
    """Past its deadline or not, an occupied nonce is no proof the bytes were never mined;
    the transaction that took it is."""
    setting = v3_setting
    start = setting.balances()
    record, _ = v3.stopped_with_the_swap_unmined(setting, monkeypatch, capsys)

    result = released_once_final(setting.fork, setting.recover, record, setting.owner)
    assert ("released_expired" in result, setting.balances()) == (False, start)
    assert setting(execute=True)["completed"]


def test_a_lending_approval_whose_nonce_was_taken_is_released(desk, monkeypatch, capsys):
    record = lending.stopped_after_the_first_send(desk, monkeypatch, capsys)
    lending.assert_blocked(desk, record)
    owner = desk.wallet["address"].lower()

    result = released_once_final(desk.fork, desk.recover, record, owner)
    assert lending.operations(result) == []
    # Nothing was approved, so the supply stated again takes both of its steps.
    assert lending.ran(desk("supply", lending.AMOUNT, execute=True), "approve", "supply")
