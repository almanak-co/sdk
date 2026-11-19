"""`almanak ax supply` and `ax withdraw` on deployed Morpho Blue markets, one process each.

An isolated Anvil fork of each chain stands in for its mainnet behind the production
adapter's own planning, signing and reconciliation. Every command resolves the market
and reads the wallet's position again, as the real command does. It is rehearsal
evidence, not a mainnet run.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.configuration.parameters import construct_parameters
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.lending_lookup import selection
from almanak.connectors.morpho_blue.direct import OTHER_LEG
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    SupplyOperation,
    plan_call,
)
from almanak.connectors.morpho_blue.support import DIRECT_CHAINS
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.safe_funding import fund_asset, native_wrapping_balance
from almanak.models import Account, AssetAmount
from tests.fork.production_gateway_driver import RehearsalGateway
from tests.strategy_fork.adapter import resolve_reference

ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.fork
PASSWORD = "public-test-password"
# The reference lending strategy of each chain names the market and its fork fixture.
REFERENCES = {
    "eip155:1": "ethereum",
    "eip155:8453": "base",
    "eip155:42161": "arbitrum",
    "eip155:10": "optimism",
    "eip155:137": "polygon",
    "eip155:4663": "robinhood",
    "eip155:56": "bnb",
    "eip155:43114": "avalanche",
    "eip155:999": "hyperevm",
}
CHAINS = tuple(chain for chain in REFERENCES if chain in DIRECT_CHAINS)
LIMITS = SimpleNamespace(
    connector="morpho_blue",
    public_rpc=None,
    # Above every fork origin's quote: Polygon's was 529 gwei at its block.
    max_gas_price_wei=5000 * 10**9,
    max_transaction_fee=10**18,
    max_gas_limit=None,
)


def test_every_declared_chain_is_rehearsed():
    assert set(CHAINS) == set(DIRECT_CHAINS)


class Reference:
    """The chain's reference market, its fork origin and the fixture's funding."""

    def __init__(self, chain, env, directory):
        name = f"morpho_blue_{REFERENCES[chain]}_lending"
        author = ROOT / "strategies" / name / "config.yaml"
        operator = ROOT / "examples/operators" / (name + ".fork.yaml")
        progress = resolve_reference(author, operator, env, directory)
        plan = compile_fixture(
            progress.context, read_operator_config(operator), progress.result.anchors
        )
        (self.target,) = plan.forks
        self.origin = self.target.resolve(env)
        self.market = (
            construct_parameters(read_inputs(author), progress).allocations[0].reserve.market
        )
        self.funds = {fund.account: fund for fund in plan.funds}
        self.native = {item.name: item.native_balance.amount for item in plan.operators}

    def funded_signer(self, fork, account):
        """A fresh plain account that holds what the fixture gives one of its funds."""
        fund = self.funds[account]
        native = self.native[fund.operator] + native_wrapping_balance(fund)
        signer = fork.funded_signer("", 0, native)
        for asset in fund.assets:
            fund_asset(fork, signer, signer.address.lower(), asset)
        return signer

    def funded(self, account, asset):
        assets = self.funds[account].assets
        return next(item.balance.amount for item in assets if item.balance.asset == asset)


def wallet_directory(signer, directory):
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
    return ax_actions.read_wallet(directory)


def as_mainnet(fork, chain):
    fork.rpc("anvil_setChainId", [chain_number(chain)])
    fork.rpc.signing_chain = chain
    # Fee history must include a locally mined block.
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


class Rehearsal:
    def __init__(self, fork, reference, directory):
        self.fork, self.market = fork, reference.market
        self.signer = reference.funded_signer(fork, "borrower")
        self.lender = reference.funded_signer(fork, "lender")
        self.liquidity = reference.funded("lender", self.market.loan_asset)
        self.wallet = wallet_directory(self.signer, directory)
        self.amounts = {
            "loan": reference.funded("borrower", self.market.loan_asset) // 4,
            "collateral": reference.funded("borrower", self.market.collateral_asset) // 4,
        }
        self.identity = as_mainnet(fork, self.market.chain.chain_id)

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

    def configuration(self, leg, action):
        """What the command builds: the market resolved and the wallet read at the head."""
        chain = self.market.chain.chain_id
        request = selection(
            "morpho_blue",
            {"chain": str(chain), "market_id": str(self.market.market_id), "leg": leg},
        )
        return ax_actions.action_config(
            LIMITS, request, action, self.wallet, ax_actions.alchemy(chain), self.fork.rpc.endpoint
        )

    def __call__(self, action, leg="loan", *, amount=None, execute):
        if action == "supply" and amount is None:
            amount = self.amounts[leg]
        request = ActionRequest(action, amount)
        config = self.configuration(leg, request)
        gateway = self.backend(config)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(record, self.backend)

    def send(self, signer, step):
        call = plan_call(step)
        self.fork.rpc.send(signer, to_checksum_address(call.to), call.data)

    def approve(self, amount):
        """Leave an allowance to the core that no request of this wallet asked for."""
        target = self.configuration("loan", ActionRequest("withdraw")).target(ALLOCATION)
        stray = AssetAmount(asset=target.asset, amount=amount)
        self.send(self.signer, ApproveOperation(target=target, amount=stray, purpose="supply"))

    def borrow(self, amount):
        """Open debt as another tool would: no direct action borrows.

        Another account lends first, so a market with no idle liquidity can pay it out
        and still return the wallet's own supply in full.
        """
        target = self.configuration("loan", ActionRequest("withdraw")).target(ALLOCATION)
        fund = Account(chain=target.fund.chain, address=self.lender.address.lower())
        lender = target.model_copy(update={"fund": fund})
        lent = AssetAmount(asset=target.asset, amount=self.liquidity)
        self.send(self.lender, ApproveOperation(target=lender, amount=lent, purpose="supply"))
        self.send(self.lender, SupplyOperation(target=lender, amount=lent))
        borrowed = AssetAmount(asset=target.asset, amount=amount)
        self.send(self.signer, BorrowOperation(target=target, amount=borrowed))


@pytest.fixture(params=CHAINS)
def setting(request, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    env = request.config.getoption("--fork-env").resolve()
    reference = Reference(request.param, env, tmp_path / "resolution")
    with Fork(env, reference.origin, target=reference.target) as fork:
        yield Rehearsal(fork, reference, tmp_path / "wallet")


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def held(result, moment, unit="supply_shares"):
    return int(result[moment][unit])


EMPTY_LOAN = {"leg": "loan", "supply_shares": "0", "borrow_shares": "0"}


def assert_ran(result, steps):
    """The steps the command sent, in order, and that they completed its request."""
    assert (operations(result), result["completed"]) == (steps, True)


def assert_supplied(result, before, unit="supply_shares"):
    assert_ran(result, ["approve", "supply"])
    assert held(result, "position_before", unit) == before
    assert held(result, "position_after", unit) > before


def assert_withdrawn(result, steps, unit="supply_shares"):
    assert_ran(result, steps)
    assert held(result, "position_after", unit) == 0


def assert_refused(setting, error, message, *arguments, **options):
    with pytest.raises(error, match=message):
        setting(*arguments, **options)


def assert_preview(preview):
    assert (preview["executed"], "outcomes" in preview) == (False, False)
    assert preview["first_step"]["operation"]["kind"] == "approve"
    assert preview["position_before"] == EMPTY_LOAN


def test_supply_then_withdraw_each_as_one_command(setting):
    assert_preview(setting("supply", execute=False))
    first = setting("supply", execute=True)
    assert_supplied(first, 0)
    # A later command adopts the position the account already holds and adds to it.
    assert_supplied(setting("supply", execute=True), held(first, "position_after"))
    assert_withdrawn(setting("withdraw", execute=True), ["withdraw"])


def stopped_after_the_first_send(setting, monkeypatch, capsys):
    """Run a supply that is interrupted while its approval waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            setting("supply", execute=True)
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def assert_blocked(setting, record):
    (listed,) = ax_actions.kept_records()
    assert (listed["path"], listed["unresolved"]["operation"]) == (str(record), "approve")
    # The kept record blocks the wallet until its signed step is resolved.
    assert_refused(setting, RuntimeError, "already claimed", "supply", execute=False)


def assert_recovered(recovered, record):
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (operations(recovered), "unresolved" in recovered) == (["approve"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)


def test_an_interrupted_supply_is_recovered_and_the_wallet_is_freed(setting, monkeypatch, capsys):
    record = stopped_after_the_first_send(setting, monkeypatch, capsys)
    assert_blocked(setting, record)
    assert_recovered(setting.recover(record), record)
    # The approval stands: another amount is refused, and the same supply needs only
    # its own step.
    refusal = "supply allowance differs from the requested amount"
    other = setting.amounts["loan"] + 1
    assert_refused(setting, DataError, refusal, "supply", amount=other, execute=False)
    assert_ran(setting("supply", execute=True), ["supply"])
    # A withdrawal is complete only once an allowance left on the core is revoked too.
    setting.approve(7)
    assert_withdrawn(setting("withdraw", execute=True), ["withdraw", "revoke"])


def assert_posted(posted, amount):
    """A first collateral supply: the whole amount, and no other leg bound yet."""
    assert_supplied(posted, 0, "collateral")
    assert held(posted, "position_after", "collateral") == amount
    assert "other_leg" not in posted["position_after"]


def assert_lent_beside(lent, collateral):
    """A first loan supply whose deployment binds the collateral the wallet holds."""
    assert_supplied(lent, 0)
    bound = (lent["position_before"]["other_leg"], lent["position_after"]["other_leg"])
    assert bound == (collateral, collateral)


def test_each_leg_is_served_while_the_wallet_holds_the_other(setting):
    posted = setting("supply", "collateral", execute=True)
    assert_posted(posted, setting.amounts["collateral"])
    lent = setting("supply", "loan", execute=True)
    assert_lent_beside(lent, posted["position_after"])
    released = setting("withdraw", "collateral", execute=True)
    assert_withdrawn(released, ["withdraw"], "collateral")
    assert released["position_after"]["other_leg"]["supply_shares"] == str(
        held(lent, "position_after")
    )
    withdrawn = setting("withdraw", "loan", execute=True)
    assert_ran(withdrawn, ["withdraw"])
    assert withdrawn["position_after"] == EMPTY_LOAN


def assert_bound_without_authority(config):
    """Both legs of the market, neither with borrow authority or a health floor."""
    assert [item.name for item in config.allocations] == [ALLOCATION, OTHER_LEG]
    policies = {(item.borrow, item.minimum_health_factor) for item in config.allocations}
    assert policies == {(False, None)}


def test_a_wallet_with_debt_keeps_its_collateral_and_every_safe_action(setting):
    setting("supply", "collateral", execute=True)
    setting.borrow(setting.amounts["loan"] // 100)
    assert_bound_without_authority(setting.configuration("collateral", ActionRequest("withdraw")))
    refusal = "cannot be withdrawn in full while that debt is open"
    assert_refused(setting, DataError, refusal, "withdraw", "collateral", execute=True)
    more = setting("supply", "collateral", execute=True)
    assert_ran(more, ["approve", "supply"])
    assert int(more["position_after"]["other_leg"]["borrow_shares"]) > 0
    lent = setting("supply", "loan", execute=True)
    assert_ran(lent, ["approve", "supply"])
    owed = held(lent, "position_before", "borrow_shares")
    assert owed > 0
    withdrawn = setting("withdraw", "loan", execute=True)
    assert_withdrawn(withdrawn, ["withdraw"])
    assert held(withdrawn, "position_after", "borrow_shares") == owed
