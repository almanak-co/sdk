"""`almanak ax supply` and `ax withdraw` on deployed Aave V3 and SparkLend Pools.

One isolated Anvil fork per chain stands in for mainnet behind the production adapter's
own planning, signing and reconciliation, at the block each reference's fork fixture
pins and, for Aave V3, again at a block where the Pool runs revision 12. The market is
resolved and the deployment built by the command's own code. It is rehearsal evidence,
not a mainnet run.
"""

import json
from pathlib import Path

import pytest
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.configuration.yaml import load
from almanak.connectors.aave_v3.networks import POOLS
from almanak.connectors.direct import ActionRequest
from almanak.connectors.lending_lookup import selection
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import Chain
from tests.fork.aave_v3_revision12 import ANCHORS, COLLATERAL, pool_revision
from tests.fork.production_gateway_driver import RehearsalGateway

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
AMOUNT = 10**16
PASSWORD = "public-test-password"
# Above the fee market of every pinned block; Polygon's quotes about 580 gwei.
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=5000 * 10**9, max_transaction_fee=10**19)
# Each declared cell, by the reference whose fork fixture pins its block and assets, and
# the Pool revision an Aave V3 cell runs there. The fixtures pin revision 11; every Aave V3
# cell runs again at a block after its Pool moved to revision 12.
CELLS = (
    *(("aave_v3", reference, 11) for reference in ANCHORS),
    *(("aave_v3", reference, 12) for reference in ANCHORS),
    ("sparklend", "sparklend_ethereum_lending", None),
)


def cell_id(cell):
    connector, reference, revision = cell
    return reference if revision in (None, 11) else f"{reference}-revision{revision}"


def fixture_of(reference, revision):
    """The pinned block and the second funded asset of a reference's fork fixture."""
    fork = load(ROOT / f"examples/operators/{reference}.fork.yaml").value["fork"]
    (anchor,) = fork["anchors"]
    chain = ChainId(anchor["chain"])
    wrapped = evm_network(chain).wrapped_native
    others = [AssetType(a["asset"]) for a in fork["funds"][0]["assets"]]
    if revision == 12:
        anchor = ANCHORS[reference]
    return chain, anchor, next(asset for asset in others if asset != wrapped)


def collateral_of(reference, revision):
    """The asset a cell supplies where it cannot supply the wrapped native coin."""
    return COLLATERAL.get(reference) if revision == 12 else None


def assert_revision(fork, connector, chain, revision):
    """The fork's Pool reports the revision the cell names, so no cell passes on the other."""
    if connector == "aave_v3":
        assert pool_revision(fork.rpc, POOLS[chain]) == revision


def origin_block(env, chain, anchor):
    with EthereumReader(
        Chain(chain_id=chain, environment="mainnet"), read_endpoint(chain, env)
    ) as r:
        block = r.pin_block(anchor["height"])
    assert block.hash == anchor["reference"]
    return block


def funded_wallet(fork, token, directory, wrapped=100 * AMOUNT):
    signer = fork.funded_signer(to_checksum_address(token), wrapped, 100 * 10**18)
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
    # The fork keeps the pinned block's time: the adapter's clock is the fork's own, and
    # a jump to the present would age every price feed past its staleness bound.
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


class Desk:
    """One wallet on one fork; every action is its own deployment, as each command's is."""

    def __init__(self, fork, connector, chain, second, tmp_path, collateral=None):
        self.fork, self.connector, self.chain = fork, connector, chain
        native = evm_network(chain).wrapped_native
        # `wrapped` is the asset an action names by default: the wrapped native coin,
        # or the collateral a cell states where the Pool admits no supply of that coin.
        self.wrapped, self.second = collateral or native, second
        self.wallet = funded_wallet(
            fork,
            erc20_address(native, chain),
            tmp_path / "wallet",
            0 if collateral else 100 * AMOUNT,
        )
        if collateral:
            fund_erc20(fork, self.wallet["address"], collateral, 100 * AMOUNT)
        self.identity = as_mainnet(fork, chain)

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

    def __call__(self, action, amount=None, *, execute, asset=None, floor=None, enable=None):
        market = {"chain": str(self.chain), "asset": str(asset or self.wrapped)}
        request = ActionRequest(action, amount, min_health_factor=floor, enable=enable)
        config = ax_actions.deployment(
            self.connector,
            selection(self.connector, market),
            request,
            self.wallet,
            ax_actions.alchemy(self.chain),
            self.fork.rpc.endpoint,
            LIMITS,
        )
        gateway = self.backend(config)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(record, self.backend)

    def fund_second(self, amount):
        fund_erc20(self.fork, self.wallet["address"], self.second, amount)


@pytest.fixture(params=CELLS, ids=cell_id)
def desk(request, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    connector, reference, revision = request.param
    chain, anchor, second = fixture_of(reference, revision)
    env = request.config.getoption("--fork-env").resolve()
    origin = origin_block(env, chain, anchor)
    target = ForkTarget(chain=origin.chain, provider=ax_actions.alchemy(chain), block=origin)
    with Fork(env, origin, target=target) as fork:
        assert_revision(fork, connector, chain, revision)
        yield Desk(fork, connector, chain, second, tmp_path, collateral_of(reference, revision))


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def supplied(result, moment):
    return int(result[moment]["supplied_scaled"])


def ran(result, *steps):
    """Whether a command completed by running exactly these steps, in order."""
    return (operations(result), result["completed"]) == (list(steps), True)


def assert_supplied(result, before):
    assert ran(result, "approve", "supply")
    assert supplied(result, "position_before") == before < supplied(result, "position_after")


def assert_preview(preview):
    assert (preview["executed"], "outcomes" in preview) == (False, False)
    assert preview["first_step"]["operation"]["kind"] == "approve"
    assert supplied(preview, "position_before") == 0


def assert_withdrawn(result):
    """A full withdrawal leaves nothing supplied and the reserve no longer collateral."""
    assert ran(result, "withdraw")
    after = result["position_after"]
    assert (int(after["supplied_scaled"]), after["collateral_enabled"]) == (0, False)


def test_supply_then_withdraw_each_as_one_command(desk):
    assert_preview(desk("supply", AMOUNT, execute=False))
    first = desk("supply", AMOUNT, execute=True)
    assert_supplied(first, 0)
    # A later command adopts the position the account already holds and adds to it.
    assert_supplied(desk("supply", AMOUNT, execute=True), supplied(first, "position_after"))
    assert_withdrawn(desk("withdraw", execute=True))


def test_a_wallet_that_holds_another_reserve_of_the_pool_supplies_and_withdraws(desk):
    """The Pool's risk check needs every collateral reserve of the account; the deployment
    binds the ones the wallet already holds beside the requested one."""
    assert_supplied(desk("supply", AMOUNT, execute=True), 0)
    amount = 10**5
    desk.fund_second(amount)
    assert_supplied(desk("supply", amount, execute=True, asset=desk.second), 0)
    assert_withdrawn(desk("withdraw", execute=True))
    assert_withdrawn(desk("withdraw", execute=True, asset=desk.second))


def debt(result, moment):
    return int(result[moment]["debt_scaled"])


FLOOR, BORROWED = 15 * 10**17, 10**5


def assert_supply_moved(result, steps, direction):
    """A completed command moved the supplied balance up (1) or down (-1), not to zero."""
    assert ran(result, *steps)
    before, after = supplied(result, "position_before"), supplied(result, "position_after")
    assert min(before, after) > 0
    assert (after - before) * direction > 0


def open_debt(desk):
    """Supply collateral and borrow against it; a floor no position reaches refuses."""
    # The wallet holds the borrowed asset already, so a full repayment covers interest.
    desk.fund_second(10 * BORROWED)
    # Enough of the cheapest wrapped native coin to back the debt above the floor.
    assert_supplied(desk("supply", 50 * AMOUNT, execute=True), 0)
    # A floor no position reaches, even where the borrowed amount is dust.
    with pytest.raises(DataError, match="below the configured minimum"):
        desk("borrow", BORROWED, execute=False, asset=desk.second, floor=10**60)
    opened = desk("borrow", BORROWED, execute=True, asset=desk.second, floor=FLOOR)
    assert ran(opened, "borrow")
    assert debt(opened, "position_before") == 0 < debt(opened, "position_after")


def act_with_debt_open(desk):
    """With debt, an action the Pool risk-checks needs a stated floor, and runs under one."""
    with pytest.raises(ValueError, match="state --min-health-factor"):
        desk("supply", AMOUNT, execute=False)
    more = desk("supply", AMOUNT, execute=True, floor=FLOOR)
    assert_supply_moved(more, ("approve", "supply"), 1)
    part = desk("withdraw", AMOUNT, execute=True, floor=FLOOR)
    assert_supply_moved(part, ("withdraw",), -1)


def close_debt(desk):
    """Repay part of the debt, then the whole of what is left."""
    some = desk("repay", BORROWED // 2, execute=True, asset=desk.second)
    assert (operations(some)[:2], some["completed"]) == (["approve", "repay"], True)
    assert 0 < debt(some, "position_after") < debt(some, "position_before")
    closed = desk("repay", execute=True, asset=desk.second)
    assert (closed["completed"], debt(closed, "position_after")) == (True, 0)
    assert "repay" in operations(closed)


def release_collateral(desk):
    released = desk("collateral", execute=True, enable=False)
    assert ran(released, "collateral")
    assert released["position_after"]["collateral_enabled"] is False
    assert_withdrawn(desk("withdraw", execute=True))


def test_a_debt_is_opened_and_closed_by_separate_commands(desk):
    """Borrow against supplied collateral, then every way out, each one command that
    adopts what the earlier ones left."""
    open_debt(desk)
    act_with_debt_open(desk)
    close_debt(desk)
    release_collateral(desk)


def stopped_after_the_first_send(desk, monkeypatch, capsys):
    """Run a supply that is interrupted while its approval waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            desk("supply", AMOUNT, execute=True)
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def assert_blocked(desk, record):
    """The kept record names its signed step and blocks the wallet until it is resolved."""
    (listed,) = ax_actions.kept_records()
    assert (listed["path"], listed["unresolved"]["operation"]) == (str(record), "approve")
    with pytest.raises(RuntimeError, match="already claimed"):
        desk("supply", AMOUNT, execute=False)


def assert_recovered(recovered, record):
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (operations(recovered), "unresolved" in recovered) == (["approve"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)


def test_an_interrupted_supply_is_recovered_and_the_wallet_is_freed(desk, monkeypatch, capsys):
    record = stopped_after_the_first_send(desk, monkeypatch, capsys)
    assert_blocked(desk, record)
    assert_recovered(desk.recover(record), record)
    # The approval stands, so restating the same supply needs only its own step.
    assert ran(desk("supply", AMOUNT, execute=True), "supply")
