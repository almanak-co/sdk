"""`almanak ax swap` on deployed Aerodrome stable and volatile pools, one process per command.

Both invariants of the one cell that declares the swap as a direct action are
rehearsed: an isolated Anvil fork of Base stands in for mainnet behind the production
adapter's own planning, signing and reconciliation, and the command's own resolution
builds each deployment. The fork keeps the time of its pinned block. It is rehearsal
evidence, not a mainnet run.
"""

from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import calldata, fund_erc20, read_uint
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import Chain
from tests.fork.test_ax_swap import (
    SLIPPAGE_BPS,
    Reference,
    Setting,
    assert_blocked,
    assert_no_allowance,
    assert_preview,
    assert_recovered,
    assert_refused,
    assert_refused_beyond_the_balance,
    assert_swapped,
    funded_wallet,
    left_to_router,
    operations,
    refused_after_the_approval,
    stopped_while_waiting_for,
)

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
CONNECTOR = "aerodrome"
BASE = ChainId("eip155:8453")


def declared_variants():
    """The pool invariants of the cell that declares the swap as a direct action."""
    cells = connector_from_wire({"connector": CONNECTOR}).chain_support
    return [
        str(variant)
        for support in cells
        if support.direct_actions and support.chain == BASE
        for variant in support.variants
    ]


def reference(variant: str) -> Reference:
    """The pinned block, pool and funded asset of the invariant's trading reference."""
    name = f"aerodrome_base_{variant}_trading"
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    market = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())["markets"]
    (anchor,) = fork["anchors"]
    (fund,) = fork["funds"]
    (funded,) = fund["assets"]
    (operator,) = fork["operators"]
    return Reference(
        connector=CONNECTOR,
        chain=BASE,
        pool=market["pool"]["pool"],
        height=anchor["height"],
        block_hash=anchor["reference"],
        asset=AssetType(funded["asset"]),
        funded=funded["amount"],
        native=operator["native_balance"]["amount"],
    )


@pytest.fixture(params=declared_variants())
def cell(request):
    return reference(request.param)


@pytest.fixture
def fork(request, cell):
    env = request.config.getoption("--fork-env").resolve()
    chain = Chain(chain_id=cell.chain, environment="mainnet")
    with EthereumReader(chain, read_endpoint(cell.chain, env)) as reader:
        origin = reader.pin_block(cell.height)
    assert origin.hash == cell.block_hash
    target = ForkTarget(chain=chain, provider=ax_actions.alchemy(cell.chain), block=origin)
    with Fork(env, origin, target=target) as running:
        yield running


def as_mainnet(fork, chain):
    """Answer as the chain itself. No timestamp is set: blocks follow the pinned one."""
    fork.rpc("anvil_setChainId", [chain_number(chain)])
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


@pytest.fixture
def setting(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet = funded_wallet(fork, cell, tmp_path / "wallet")
    identity = as_mainnet(fork, cell.chain)
    return Setting(fork, cell, wallet, identity)


def shares(setting):
    """The pool's own share token, and what the wallet holds and lets the router spend."""
    target = setting.target()
    token = erc20_address(target.pool.shares_asset, setting.cell.chain)
    router = target.contracts.router
    held = read_uint(setting.fork.rpc, token, "balanceOf(address)", (setting.owner,))
    granted = read_uint(
        setting.fork.rpc, token, "allowance(address,address)", (setting.owner, router)
    )
    return target.pool.shares_asset, held, granted


def test_a_swap_sells_exactly_the_stated_amount_and_leaves_no_allowance(setting):
    start = setting.balances()
    preview = setting(execute=False)
    assert_preview(setting, preview)
    # A preview signs nothing: no balance and no allowance moved.
    assert (start, setting.balances()) == ((setting.cell.funded, 0), start)
    assert (preview["position_before"]["allowances"], setting.router_allowance()) == ([], 0)

    minimum = setting.quoted_output() * (10000 - SLIPPAGE_BPS) // 10000
    first = setting(execute=True)
    assert_swapped(setting, first, start, minimum)
    assert_no_allowance(setting, first)

    # A later command is a new deployment: it sells its own stated amount again.
    middle = setting.balances()
    second = setting(execute=True)
    assert_swapped(setting, second, middle, 1)
    assert_no_allowance(setting, second)
    assert first["completed"] and second["completed"]
    assert_refused_beyond_the_balance(setting)
    # No command minted, burned or approved the pool's shares.
    assert shares(setting)[1:] == (0, 0)


def selling_back(setting):
    """The same wallet on the same fork, selling all it holds of the other currency."""
    other = next(asset for asset in setting.target().assets if asset != setting.cell.asset)
    cell = replace(setting.cell, asset=other, funded=setting.balances()[1])
    return Setting(setting.fork, cell, setting.wallet, setting.identity)


def test_the_other_currency_is_sold_back_through_the_same_pool(setting):
    first = setting(execute=True)
    sold, bought = setting.balances()
    back = selling_back(setting)
    start = back.balances()
    assert (first["completed"], start) == (True, (bought, sold))
    result = back(execute=True)
    assert_swapped(back, result, start, 1)
    assert_no_allowance(back, result)
    assert (result["completed"], setting.router_allowance()) == (True, 0)


def test_an_interrupted_swap_is_recovered_and_reports_the_allowance_it_left(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record = stopped_while_waiting_for("approve", setting, monkeypatch, capsys)
    assert_blocked(setting, record, "approve")
    recovered = setting.recover(record)
    assert_recovered(setting, recovered, record, start)
    # The approval that was mined stands, and the result says so.
    assert recovered["position_after"]["allowances"] == left_to_router(setting)

    # Restating the same swap needs only its own step, and the allowance is used up.
    again = setting(execute=True)
    assert (operations(again), again["completed"]) == (["swap"], True)
    assert again["position_before"]["allowances"] == left_to_router(setting)
    assert start[0] - setting.balances()[0] == setting.cell.amount
    assert_no_allowance(setting, again)


def test_a_swap_interrupted_after_it_was_sent_is_recovered_from_its_receipt(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record = stopped_while_waiting_for("swap", setting, monkeypatch, capsys)
    assert_blocked(setting, record, "swap")
    # The request finished with the swap's receipt, so nothing was left to drop.
    recovered = setting.recover(record)
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_swapped(setting, recovered, start, 1)
    assert_no_allowance(setting, recovered)


def test_a_swap_refused_after_its_approval_reports_the_allowance_that_remains(setting):
    start = setting.balances()
    refused = refused_after_the_approval(setting)
    assert_refused(setting, refused, start)
    assert refused["position_after"]["allowances"] == left_to_router(setting)
    assert setting.router_allowance() == setting.cell.amount

    # A swap of another amount resets the allowance that remained before its own.
    other = setting.cell.amount // 2
    again = setting(other, execute=True)
    assert (operations(again), again["completed"]) == (["approve", "approve", "swap"], True)
    assert start[0] - setting.balances()[0] == other
    assert_no_allowance(setting, again)


def test_a_wallet_that_holds_the_pools_shares_is_refused_before_anything_is_planned(setting):
    asset, _, _ = shares(setting)
    fund_erc20(setting.fork, setting.owner, asset, 1)
    start = setting.balances()
    for execute in (False, True):
        with pytest.raises(DataError, match="a trading-only allocation cannot own") as refusal:
            setting(execute=execute)
        assert refusal.value.kind == "ownership" and setting.cell.pool in str(refusal.value)
    # Nothing was signed: no record is kept, and no balance, share or allowance moved.
    assert (ax_actions.kept_records(), setting.balances()) == ([], start)
    assert (shares(setting)[1:], setting.router_allowance()) == ((1, 0), 0)


def share_sender(setting):
    """Another account that holds one base unit of the pool's shares and can pay to send it."""
    sender = "0x" + "5e" * 20
    setting.fork.rpc("anvil_setBalance", [sender, hex(10**18)])
    fund_erc20(setting.fork, sender, shares(setting)[0], 1)
    setting.fork.rpc("anvil_impersonateAccount", [sender])
    return sender


def share_sent_with_the_swap(setting, monkeypatch, *, stop=False):
    """Make the next swap share its block with a transfer of one pool share to the wallet.

    The share arrives after the swap was signed and sent, so no observation before the
    signature could see it; the swap is reconciled at the block that holds both. With
    `stop`, the command is interrupted once that block is mined.
    """
    from almanak.operator_runtime import OperatorRuntime

    rpc, sender = setting.fork.rpc, share_sender(setting)
    token = erc20_address(shares(setting)[0], setting.cell.chain)
    transfer = calldata("transfer(address,uint256)", ("address", "uint256"), (setting.owner, 1))

    def wait(self, state):
        pending = state.pending
        if pending is None or pending.operation.kind != "swap":
            return
        if pending.unsent:
            # Signed, with its last check before the send still to come: the swap then
            # stays in the node's pool until the share has been sent as well.
            rpc("evm_setAutomine", [False])
            return
        priced = {"from": sender, "to": token, "data": transfer, "gasPrice": rpc("eth_gasPrice")}
        rpc("eth_sendTransaction", [priced])
        rpc("evm_mine")
        rpc("evm_setAutomine", [True])
        interrupt_if(stop)

    monkeypatch.setattr(OperatorRuntime, "wait", wait)


def interrupt_if(stop):
    if stop:
        raise KeyboardInterrupt


def assert_share_reported(setting, result):
    """The swap is reconciled, and its outcome names the share no step explains."""
    approval, swap = result["outcomes"]
    (issue,) = swap["accounting_issues"]
    assert (swap["success"], "accounting_issues" in approval) == (True, False)
    assert str(shares(setting)[0]) in issue and "trading-only allocation" in issue
    # The share was sent to the wallet and not moved or approved by the command.
    assert shares(setting)[1:] == (1, 0)


def assert_refused_for_the_share(setting):
    """The wallet is free of any record, and its share refuses the next command at once."""
    with pytest.raises(DataError, match="a trading-only allocation cannot own") as refusal:
        setting(execute=False)
    assert (refusal.value.kind, ax_actions.kept_records()) == ("ownership", [])


def test_a_share_sent_to_the_wallet_while_the_swap_is_unmined_is_reported_not_refused(
    setting, monkeypatch
):
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_with_the_swap(setting, patched)
        result = setting(execute=True)
    assert result["completed"]
    assert_swapped(setting, result, start, 1)
    assert_no_allowance(setting, result)
    assert_share_reported(setting, result)
    assert_refused_for_the_share(setting)


def test_a_swap_mined_with_a_share_sent_to_the_wallet_is_recovered_with_the_share_reported(
    setting, monkeypatch
):
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_with_the_swap(setting, patched, stop=True)
        with pytest.raises(KeyboardInterrupt):
            setting(execute=True)
    (kept,) = ax_actions.kept_records()
    recovered = setting.recover(Path(kept["path"]))
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert_swapped(setting, recovered, start, 1)
    assert_no_allowance(setting, recovered)
    assert_share_reported(setting, recovered)
    assert_refused_for_the_share(setting)


def test_both_invariants_are_declared_and_have_a_trading_reference_to_rehearse():
    variants = declared_variants()
    found = [reference(variant) for variant in variants]
    assert variants == ["stable", "volatile"]
    assert len({item.pool for item in found}) == 2
    assert all(item.amount > 0 and item.asset.chain == item.chain for item in found)
