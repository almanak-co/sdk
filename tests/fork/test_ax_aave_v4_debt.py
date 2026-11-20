"""`almanak ax borrow` and `ax repay` on deployed Aave V4 Spokes, one command each.

One isolated Anvil fork per chain stands in for mainnet behind the production adapter's
own planning, signing and reconciliation, at the block the borrowing reference's fork
fixture pins. The market is resolved and the deployment built by the command's own code.
It is rehearsal evidence, not a mainnet run.
"""

import json
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import NamedTuple

import pytest
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.configuration.yaml import load
from almanak.connectors.aave_v4 import execution as aave_execution
from almanak.connectors.direct import ActionRequest
from almanak.connectors.lending_lookup import selection
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.read_endpoints import read_endpoint
from almanak.gateway.safe_fixture import calldata
from almanak.models import Chain
from tests.fork.production_gateway_driver import RehearsalGateway

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
REFERENCE = "aave_v4_multichain_borrowing"
# Base units of the wrapped native asset supplied as collateral, and of the borrowed
# asset, which has six decimals on both chains.
COLLATERAL, BORROWED = 10**18, 10**6
FLOOR = 15 * 10**17
# A floor no position meets.
UNREACHABLE = 10**6 * 10**18
PASSWORD = "public-test-password"
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=5000 * 10**9, max_transaction_fee=10**19)
# Per chain of the reference: a second reserve of its Spoke with a collateral factor and
# one without, at the pinned block.
CELLS = {"ethereum": (8, 9), "avalanche": (3, 5)}


class Cell(NamedTuple):
    chain: ChainId
    anchor: dict
    spoke: str
    # Reserve IDs: the reference's collateral and debt, then the two named in CELLS.
    collateral: int
    debt: int
    second: int
    inert: int
    borrowed: AssetType


def borrowed_asset(fork, chain):
    """The asset the reference's fork fixture funds on the chain beside the wrapped one."""
    funded = {AssetType(a["asset"]) for fund in fork["funds"] for a in fund["assets"]}
    wrapped = evm_network(chain).wrapped_native
    (borrowed,) = {asset for asset in funded if asset.chain == chain} - {wrapped}
    return borrowed


def fixture_of(name):
    """One chain's pinned block, Spoke, reserve IDs and borrowed asset in the reference."""
    fork = load(ROOT / f"examples/operators/{REFERENCE}.fork.yaml").value["fork"]
    markets = load(ROOT / f"strategies/{REFERENCE}/config.yaml").value["markets"]
    collateral, debt = markets[f"{name}_collateral_market"], markets[f"{name}_debt_market"]
    chain = ChainId(collateral["chain"])
    (anchor,) = [item for item in fork["anchors"] if item["chain"] == str(chain)]
    assert collateral["spoke"] == debt["spoke"]
    reserves = (collateral["reserve_id"], debt["reserve_id"], *CELLS[name])
    return Cell(chain, anchor, collateral["spoke"], *reserves, borrowed_asset(fork, chain))


def origin_block(env, chain, anchor):
    with EthereumReader(
        Chain(chain_id=chain, environment="mainnet"), read_endpoint(chain, env)
    ) as r:
        block = r.pin_block(anchor["height"])
    assert block.hash == anchor["reference"]
    return block


def funded_wallet(fork, token, directory):
    signer = fork.funded_signer(to_checksum_address(token), 4 * COLLATERAL, 100 * 10**18)
    directory.mkdir(mode=0o700)
    stores = {"treasury": Signer.encrypt(signer.key, PASSWORD, kdf="pbkdf2", iterations=1000)}
    write_private(directory / "keystores.json", json.dumps(stores))
    write_private(directory / "keystore-password", PASSWORD)
    wallet = {
        "account": "treasury",
        "address": signer.address,
        "keystores": str(directory / "keystores.json"),
        "password_file": str(directory / "keystore-password"),
    }
    write_private(directory / "wallet.json", json.dumps(wallet))
    return ax_actions.read_wallet(directory), signer


def as_mainnet(fork, chain):
    fork.rpc("anvil_setChainId", [chain_number(chain)])
    # The fork keeps the pinned block's time: the adapter's clock is the fork's own, and
    # a jump to the present would age every price feed past its staleness bound.
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


class Desk:
    """One wallet on one fork; every action is its own deployment, as each command's is."""

    def __init__(self, fork, cell, tmp_path):
        self.fork, self.cell = fork, cell
        self.collateral, self.debt = cell.collateral, cell.debt
        wrapped = erc20_address(evm_network(cell.chain).wrapped_native, cell.chain)
        self.wallet, self.signer = funded_wallet(fork, wrapped, tmp_path / "wallet")
        self.identity = as_mainnet(fork, cell.chain)

    def backend(self, config):
        return RehearsalGateway(
            config,
            self.fork.rpc.endpoint,
            self.identity,
            Path(self.wallet["keystores"]),
            Path(self.wallet["password_file"]),
            None,
            fixture_clock=True,
        )

    def deployment(self, request, reserve_id):
        cell = self.cell
        market = {"chain": str(cell.chain), "spoke": cell.spoke, "reserve_id": reserve_id}
        return ax_actions.deployment(
            "aave_v4",
            selection("aave_v4", market),
            request,
            self.wallet,
            ax_actions.alchemy(cell.chain),
            self.fork.rpc.endpoint,
            LIMITS,
        )

    def __call__(self, action, amount=None, *, execute=True, reserve=None, floor=None):
        """Run one command on the collateral reserve, or on `reserve`."""
        request = ActionRequest(action, amount, min_health_factor=floor)
        config = self.deployment(request, self.collateral if reserve is None else reserve)
        return self.run(config, request, execute=execute)

    def run(self, config, request, *, execute=True):
        """Run a request against a deployment, which may have been bound earlier."""
        gateway = self.backend(config)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def fund(self, reserve_id, amount):
        """Fund the wallet with a reserve's asset, as the command's own resolution names it."""
        config = self.deployment(ActionRequest("supply", amount), reserve_id)
        asset = config.allocations[0].reserve.asset
        fund_erc20(self.fork, self.wallet["address"], asset, amount)

    def spoke(self, signature, *arguments):
        """A Spoke call the wallet signs itself, outside any command."""
        types = tuple(signature[signature.index("(") + 1 : -1].split(","))
        self.send(self.cell.spoke, signature, types, arguments)

    def send(self, to, signature, types, arguments):
        """Sign and mine one wallet transaction, with gas to spare over the estimate:
        interest accrues between the estimate and the block that includes it."""
        rpc, sender = self.fork.rpc, self.signer.address
        data = calldata(signature, types, arguments)
        estimate = rpc("eth_estimateGas", [{"from": sender, "to": to, "data": data}])
        signed = self.signer.sign_transaction(
            {
                "chainId": chain_number(self.cell.chain),
                "nonce": int(rpc("eth_getTransactionCount", [sender, "pending"]), 16),
                "to": to_checksum_address(to),
                "data": data,
                "value": 0,
                "gas": 2 * int(estimate, 16),
                "gasPrice": int(rpc("eth_gasPrice"), 16),
            }
        )
        rpc.wait_receipt(rpc("eth_sendRawTransaction", ["0x" + signed.raw_transaction.hex()]))

    def open_debt(self, reserve_id, *collateral):
        """Enable the given reserves as collateral and borrow `reserve_id`, as raw
        transactions the binding of a command never saw."""
        owner = self.wallet["address"]
        for enabled in collateral:
            self.spoke("setUsingAsCollateral(uint256,bool,address)", enabled, True, owner)
        self.spoke("borrow(uint256,uint256,address)", reserve_id, BORROWED, owner)


@pytest.fixture(params=CELLS)
def desk(request, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    cell = fixture_of(request.param)
    env = request.config.getoption("--fork-env").resolve()
    origin = origin_block(env, cell.chain, cell.anchor)
    target = ForkTarget(chain=origin.chain, provider=ax_actions.alchemy(cell.chain), block=origin)
    with Fork(env, origin, target=target) as fork:
        desk = Desk(fork, cell, tmp_path)
        # The wallet holds the borrowed asset already, so a full repayment covers interest.
        fund_erc20(fork, desk.wallet["address"], cell.borrowed, 10 * BORROWED)
        yield desk


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def summary(result):
    """What one executed command did: its steps in order, and whether it completed."""
    return operations(result), result["completed"]


def shares(result, moment, kind):
    return int(result[moment][f"{kind}_shares"])


def refuses(desk, error, text, *arguments, **options):
    """Preview a command that is refused, and return the refusal."""
    with pytest.raises(error, match=text) as refused:
        desk(*arguments, execute=False, **options)
    return refused.value


def borrow(desk, floor=FLOOR, **options):
    return desk("borrow", BORROWED, reserve=desk.debt, floor=floor, **options)


def supplied(desk, amount=COLLATERAL, reserve=None):
    result = desk("supply", amount, reserve=reserve)
    assert summary(result) == (["approve", "supply"], True)
    return result


def withdrawn(desk, reserve=None):
    result = desk("withdraw", reserve=reserve)
    assert summary(result) == (["withdraw"], True)
    assert shares(result, "position_after", "supplied") == 0


def repaid(desk, reserve, released):
    """Repay one reserve in full: approve, repay, revoke, then one `collateral` step for
    each reserve named in `released`."""
    result = desk("repay", reserve=reserve)
    assert summary(result) == (["approve", "repay", "revoke", *released], True)
    assert shares(result, "position_before", "debt") > 0 == shares(result, "position_after", "debt")
    return result


# Refusals of the binding, read before any deployment exists.
NO_COLLATERAL = "has supplied no collateral on this Spoke"
BACKS_DEBT = "is collateral for the wallet's debt in reserve"
ONE_DEBT = "serves one debt reserve per wallet and Spoke"
# Refusals the connector raises when it plans a step.
NOTHING_LEFT = "no remaining debt, allowance or collateral"
BELOW_FLOOR = "projected account health is below"
OWED_RESERVE = "observed exposure differs from owned supply"


def test_a_debt_is_opened_and_closed_by_separate_commands(desk):
    """Borrow against supplied collateral, repay in full, then withdraw the collateral,
    each one command that adopts what the earlier ones left."""
    refuses(desk, ValueError, NO_COLLATERAL, "borrow", BORROWED, reserve=desk.debt, floor=FLOOR)
    supplied(desk)
    assert_opened(desk)
    assert_the_floor_refuses_more(desk)
    assert_held_while_indebted(desk)
    repaid(desk, desk.debt, ["collateral"])
    # The repayment released the collateral, and nothing is left for another one.
    refuses(desk, ValueError, NOTHING_LEFT, "repay", reserve=desk.debt)
    withdrawn(desk)


def assert_opened(desk):
    """A supply is not collateral on a Spoke until the borrow enables it."""
    preview = borrow(desk, execute=False)
    assert preview["first_step"]["operation"]["kind"] == "collateral"
    opened = borrow(desk)
    assert summary(opened) == (["collateral", "borrow"], True)
    assert shares(opened, "position_before", "debt") == 0 < shares(opened, "position_after", "debt")


def assert_the_floor_refuses_more(desk):
    """The collateral is enabled now, so a borrow the floor refuses signs nothing."""
    with pytest.raises(DataError, match=BELOW_FLOOR) as refused:
        borrow(desk, floor=UNREACHABLE)
    assert refused.value.kind == "risk_limit"


def assert_held_while_indebted(desk):
    """What the other commands do while the debt is open."""
    refuses(desk, ValueError, BACKS_DEBT, "withdraw")
    # More collateral is always admitted: a supply never lowers the account's health.
    more = supplied(desk)
    assert shares(more, "position_before", "supplied") < shares(more, "position_after", "supplied")
    second = desk.cell.second
    refuses(desk, ValueError, ONE_DEBT, "borrow", BORROWED, reserve=second, floor=FLOOR)
    # The connector binds one position per reserve, so the owed reserve takes no supply.
    owed = refuses(desk, DataError, OWED_RESERVE, "supply", BORROWED, reserve=desk.debt)
    assert owed.kind == "ownership_mismatch"


def refused_borrow(desk):
    """A first borrow the floor refuses: its collateral step is mined, the borrow is not."""
    result = borrow(desk, floor=UNREACHABLE)
    assert summary(result) == (["collateral"], False)
    assert result["refused"]["kind"] == "risk_limit"
    assert shares(result, "position_after", "debt") == 0


def test_a_first_borrow_the_floor_refuses_leaves_collateral_that_still_withdraws(desk):
    """The collateral is enabled before the borrow is projected against the floor."""
    supplied(desk)
    refused_borrow(desk)
    # A repayment with no debt releases the collateral the refused borrow enabled.
    released = desk("repay", reserve=desk.debt)
    assert summary(released) == (["collateral"], True)
    refused_borrow(desk)
    # With no debt on the Spoke, enabled collateral withdraws as any supply does.
    withdrawn(desk)


def test_a_borrow_enables_every_supply_that_can_back_it(desk):
    """Every reserve the wallet supplies is part of the debt's position; the one without
    a collateral factor backs nothing and is left as it is."""
    supplied(desk)
    for reserve in (desk.cell.second, desk.cell.inert):
        desk.fund(reserve, BORROWED)
        supplied(desk, BORROWED, reserve)
    assert summary(borrow(desk)) == (["collateral", "collateral", "borrow"], True)
    # A supply that is not collateral withdraws while the debt is open.
    withdrawn(desk, desk.cell.inert)
    refuses(desk, ValueError, BACKS_DEBT, "withdraw", reserve=desk.cell.second)
    repaid(desk, desk.debt, ["collateral", "collateral"])
    withdrawn(desk, desk.cell.second)
    withdrawn(desk)


def refused_by_the_connector(desk, config, request):
    """Run a request the connector refuses when it plans the first step: the refusal is
    the execution check's, not the binding's `ValueError`."""
    with pytest.raises(DataError, match="is collateral and the account owes reserve") as refused:
        desk.run(config, request)
    assert refused.value.kind == "residual_debt"
    # Nothing was signed, so no record is kept.
    assert ax_actions.kept_records() == []


def test_a_debt_opened_after_the_binding_holds_the_collateral_at_planning(desk):
    """The deployment was bound while the wallet owed nothing. The connector reads the
    account again when it plans the withdrawal, and refuses it."""
    supplied(desk)
    request = ActionRequest("withdraw")
    bound_before = desk.deployment(request, desk.collateral)
    assert summary(borrow(desk)) == (["collateral", "borrow"], True)
    refused_by_the_connector(desk, bound_before, request)
    # The same holds for a debt the wallet opened outside any command.
    repaid(desk, desk.debt, ["collateral"])
    bound_before = desk.deployment(request, desk.collateral)
    desk.open_debt(desk.cell.second, desk.collateral)
    refused_by_the_connector(desk, bound_before, request)
    # A command bound now names the cause before any deployment exists.
    refuses(desk, ValueError, BACKS_DEBT, "withdraw")


def after_planning(act):
    """An interrupt check that acts once, between the first plan and its signature."""
    calls = []

    @contextmanager
    def interruptible():
        def check():
            calls.append(None)
            if len(calls) == 2:
                act()

        yield check

    return interruptible


def signing_refusals(monkeypatch):
    """Each refusal of the connector's release check at signing, as it is raised."""
    seen = []
    checks = aave_execution.current_release_checks

    def recorded(backend, plan, block=None):
        try:
            checks(backend, plan, block)
        except DataError as error:
            seen.append((plan.operation.kind, error.kind))
            raise

    monkeypatch.setattr(aave_execution, "current_release_checks", recorded)
    return seen


def assert_nothing_signed(result):
    """The refused withdrawal left no step, signed or mined, and the supply as it was."""
    assert (result["outcomes"], result["completed"], "unresolved" in result) == ([], False, False)
    assert result["refused"]["kind"] == "residual_debt"
    assert shares(result, "position_after", "supplied") > 0


def test_a_debt_opened_after_planning_refuses_the_withdrawal_at_signing(desk, monkeypatch):
    """The plan was made and validated before the debt existed, and is not signed."""
    supplied(desk)
    seen = signing_refusals(monkeypatch)
    opening = after_planning(partial(desk.open_debt, desk.debt, desk.collateral))
    monkeypatch.setattr(ax_actions, "interruptible", opening)
    result = desk("withdraw")
    assert result["first_step"]["operation"]["kind"] == "withdraw"
    assert seen[0] == ("withdraw", "residual_debt")
    assert_nothing_signed(result)


def test_each_of_two_debts_is_repaid_and_the_collateral_stays_until_the_last(desk):
    second = desk.cell.second
    supplied(desk)
    assert borrow(desk)["completed"]
    # The second debt is opened outside any command: a direct borrow refuses one.
    desk.fund(second, 10 * BORROWED)
    desk.open_debt(second)
    repaid(desk, desk.debt, [])
    # The collateral still backs the other debt.
    refuses(desk, ValueError, f"wallet's debt in reserve {second} of this Spoke", "withdraw")
    repaid(desk, second, ["collateral"])
    withdrawn(desk)


def test_a_debt_with_no_other_supply_is_repaid(desk):
    """Supply and debt in one reserve, with nothing else on the Spoke to bind."""
    owner, backing = desk.wallet["address"], 5 * BORROWED
    token = erc20_address(desk.cell.borrowed, desk.cell.chain)
    desk.send(token, "approve(address,uint256)", ("address", "uint256"), (desk.cell.spoke, backing))
    desk.spoke("supply(uint256,uint256,address)", desk.debt, backing, owner)
    desk.open_debt(desk.debt, desk.debt)
    closed = repaid(desk, desk.debt, [])
    assert shares(closed, "position_after", "supplied") > 0
    # The reserve's own collateral flag stands, and with nothing owed it withdraws.
    withdrawn(desk, desk.debt)
