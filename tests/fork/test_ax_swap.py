"""`almanak ax swap` on deployed V3-family pools, one process per command.

Every cell that declares the swap as a direct action is rehearsed: an isolated Anvil
fork of the chain stands in for mainnet behind the production adapter's own planning,
signing and reconciliation, and the command's own resolution builds each deployment.
It is rehearsal evidence, not a mainnet run.
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from eth_account import Account as Signer

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import balance, fund_erc20, read_uint
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import AssetAmount, Chain
from almanak.transaction_family import is_unsent
from tests.fork.production_gateway_driver import RehearsalGateway

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
PASSWORD = "public-test-password"
SLIPPAGE_BPS = 100
CONNECTORS = ("uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream")
# The chain part of each trading reference's name.
NETWORKS = {
    "eip155:1": "ethereum",
    "eip155:8453": "base",
    "eip155:42161": "arbitrum",
    "eip155:10": "optimism",
    "eip155:137": "polygon",
    "eip155:43114": "avalanche",
    "eip155:56": "bnb",
    "eip155:4663": "robinhood",
}
# Wide enough for every chain's pinned block: Polygon quotes above 500 gwei at its own.
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=2000 * 10**9, max_transaction_fee=5 * 10**17)


def declared_cells():
    """Every connector and chain that declares the swap as a direct action."""
    return [
        (name, str(support.chain))
        for name in CONNECTORS
        for support in connector_from_wire({"connector": name}).chain_support
        if support.direct_actions
    ]


@dataclass(frozen=True)
class Reference:
    """The pinned block, pool and funded asset of a cell's trading reference."""

    connector: str
    chain: ChainId
    pool: str
    height: int
    block_hash: str
    asset: AssetType
    # Base units of `asset` the wallet starts with; each swap sells a quarter.
    funded: int
    native: int

    @property
    def amount(self) -> int:
        return self.funded // 4


def reference(connector: str, chain: str) -> Reference:
    name = f"{connector}_{NETWORKS[chain]}_trading"
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    market = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())["markets"]
    (anchor,) = fork["anchors"]
    (fund,) = fork["funds"]
    (funded,) = fund["assets"]
    (operator,) = fork["operators"]
    return Reference(
        connector=connector,
        chain=ChainId(chain),
        pool=market["pool"]["pool"],
        height=anchor["height"],
        block_hash=anchor["reference"],
        asset=AssetType(funded["asset"]),
        funded=funded["amount"],
        native=operator["native_balance"]["amount"],
    )


@pytest.fixture(params=declared_cells(), ids=lambda cell: f"{cell[0]}-{NETWORKS[cell[1]]}")
def cell(request):
    return reference(*request.param)


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


def funded_wallet(fork, cell, directory):
    signer = fork.funded_signer(None, 0, cell.native)
    fund_erc20(fork, signer.address.lower(), cell.asset, cell.funded)
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
    fork.rpc("evm_setNextBlockTimestamp", [int(time.time())])
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


def backend(fork, config, wallet, identity):
    return RehearsalGateway(
        config,
        fork.rpc.endpoint,
        identity,
        Path(wallet["keystores"]),
        Path(wallet["password_file"]),
        None,
        fixture_clock=True,
    )


class Setting:
    """One funded wallet on one fork; each call is one command with its own deployment."""

    def __init__(self, fork, cell, wallet, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        resolution = ax_actions.resolution(cell.connector)
        self.market = resolution.MarketSelection(
            connector=cell.connector, chain=cell.chain, pool=cell.pool
        )
        self.owner = wallet["address"].lower()

    def request(self, amount=None):
        return ActionRequest(
            "swap",
            self.cell.amount if amount is None else amount,
            sell=str(self.cell.asset),
            slippage_bps=SLIPPAGE_BPS,
            deadline_seconds=300,
        )

    def config(self, amount=None, limits=LIMITS):
        """The deployment as the command builds it, resolution included."""
        return ax_actions.deployment(
            self.cell.connector,
            self.market,
            self.request(amount),
            self.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )

    def __call__(self, amount=None, *, execute, limits=LIMITS):
        config = self.config(amount, limits)
        gateway = backend(self.fork, config, self.wallet, self.identity)
        try:
            return ax_actions.run(config, gateway, self.request(amount), execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(
            record, lambda stored: backend(self.fork, stored, self.wallet, self.identity)
        )

    def target(self):
        return self.config().target(ALLOCATION)

    def balances(self):
        """The wallet's balances of the sold and the bought currency, in base units."""
        target = self.target()
        bought = next(asset for asset in target.assets if asset != self.cell.asset)
        return tuple(
            balance(self.fork.rpc, erc20_address(asset, self.cell.chain), self.owner)
            for asset in (self.cell.asset, bought)
        )

    def router_allowance(self):
        token = erc20_address(self.cell.asset, self.cell.chain)
        router = self.target().contracts.router
        return read_uint(self.fork.rpc, token, "allowance(address,address)", (self.owner, router))

    def quoted_output(self):
        """The pool's own quote for the stated amount at the fork's head."""
        from importlib import import_module

        observations = import_module(f"almanak.connectors.{self.cell.connector}.observations")
        target = self.target()
        chain = Chain(chain_id=self.cell.chain, environment="mainnet")
        with EthereumReader(chain, self.fork.rpc.endpoint) as reader:
            quote = observations.measured_quote(
                reader,
                target,
                reader.pin_block("latest"),
                AssetAmount(asset=self.cell.asset, amount=self.cell.amount),
            )
        return quote.amount_out.amount


@pytest.fixture
def setting(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet = funded_wallet(fork, cell, tmp_path / "wallet")
    identity = as_mainnet(fork, cell.chain)
    return Setting(fork, cell, wallet, identity)


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def inventory(result, moment):
    return {item["asset"]: int(item["amount"]) for item in result[moment]["inventory"]}


def left_to_router(setting):
    """The allowance an approval of the stated amount leaves, as a result reports it."""
    cell = setting.cell
    router = setting.target().contracts.router
    return [{"asset": str(cell.asset), "spender": router, "amount": str(cell.amount)}]


def assert_preview(setting, preview):
    cell = setting.cell
    step = preview["first_step"]["operation"]
    held = inventory(preview, "position_before")
    assert (preview["action"], preview["executed"]) == ("swap", False)
    assert "outcomes" not in preview and "position_after" not in preview
    assert (step["kind"], step["amount"]) == ("approve", cell.amount)
    assert held[str(cell.asset)] == sum(held.values()) == cell.amount


def assert_swapped(setting, result, before, minimum):
    """The wallet moved by exactly the sold amount and by what the result reports bought."""
    cell = setting.cell
    after = setting.balances()
    held = inventory(result, "position_after")
    bought = sum(held.values()) - held[str(cell.asset)]
    assert operations(result) == ["approve", "swap"]
    assert "refused" not in result and "unresolved" not in result
    assert (held[str(cell.asset)], before[0] - after[0]) == (0, cell.amount)
    assert after[1] - before[1] == bought >= minimum > 0


def assert_no_allowance(setting, result):
    assert (result["position_after"]["allowances"], setting.router_allowance()) == ([], 0)


def assert_refused_beyond_the_balance(setting):
    """More than the wallet holds is refused before anything is planned."""
    held = setting.balances()[0]
    with pytest.raises(DataError, match="the wallet holds") as refusal:
        setting(held + 1, execute=True)
    assert (refusal.value.kind, setting.balances()[0]) == ("inventory", held)


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


def stopped_while_waiting_for(kind, setting, monkeypatch, capsys):
    """Run a swap that is interrupted while its step of this kind waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None and state.pending.operation.kind == kind:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            setting(execute=True)
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def assert_blocked(setting, record, kind):
    (listed,) = ax_actions.kept_records()
    assert (listed["path"], listed["unresolved"]["operation"]) == (str(record), kind)
    # The kept record blocks the wallet until its signed step is resolved.
    with pytest.raises(RuntimeError, match="already claimed"):
        setting(execute=False)


def assert_recovered(setting, recovered, record, start):
    """The signed approval is resolved, the unsigned swap dropped, and nothing was sold."""
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (operations(recovered), "unresolved" in recovered) == (["approve"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert (setting.balances(), setting.router_allowance()) == (start, setting.cell.amount)


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


def refused_after_the_approval(setting):
    """Run a swap under a gas limit that admits exactly its approval and refuses the swap."""
    approval_gas = setting(execute=False)["first_step"]["gas"]
    tight = ax_actions.FeeLimits(
        LIMITS.max_gas_price_wei, LIMITS.max_transaction_fee, max_gas_limit=approval_gas
    )
    return setting(execute=True, limits=tight)


def assert_refused(setting, refused, start):
    """Nothing is signed and unresolved, so no record is kept and the wallet is free."""
    assert (refused["executed"], refused["completed"]) == (True, False)
    assert (operations(refused), refused["refused"]["kind"]) == (["approve"], "gas_limit")
    assert "unresolved" not in refused and "record_kept" not in refused
    assert (ax_actions.kept_records(), setting.balances()) == ([], start)


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


def pausing_at_the_swap(setting, sent):
    """A wait that pauses mining once the swap is signed, and calls `sent` once it was
    sent: the swap then sits unmined with the node, as one a chain is slow to include."""

    def wait(self, state):
        pending = state.pending
        if pending is None or pending.operation.kind != "swap":
            return
        if is_unsent(pending):
            setting.fork.rpc("evm_setAutomine", [False])
        else:
            sent(pending)

    return wait


def lapse(setting, transaction):
    """The node forgets the unmined swap, and the chain moves past the swap's deadline."""
    rpc = setting.fork.rpc
    rpc("anvil_dropTransaction", [transaction])
    rpc("evm_setAutomine", [True])
    rpc("evm_increaseTime", [400])
    rpc("evm_mine")


def interrupt(pending):
    raise KeyboardInterrupt


def stopped_with_the_swap_unmined(setting, monkeypatch, capsys):
    """Interrupt a swap once it was sent, then let its deadline pass without it mined.

    Returns the kept record and the hash of the swap that will never be mined.
    """
    from almanak.operator_runtime import OperatorRuntime

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", pausing_at_the_swap(setting, interrupt))
        with pytest.raises(KeyboardInterrupt):
            setting(execute=True)
    capsys.readouterr()
    (kept,) = ax_actions.kept_records()
    lapse(setting, kept["unresolved"]["transaction"])
    return Path(kept["path"]), kept["unresolved"]["transaction"]


def executed_past_the_deadline(setting, monkeypatch):
    """Run a swap to its end while its deadline passes with the sent swap unmined."""
    from almanak.operator_runtime import OperatorRuntime

    def expire(pending):
        lapse(setting, pending.tx_hash)

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", pausing_at_the_swap(setting, expire))
        return setting(execute=True)


def assert_released(setting, result, earlier, start):
    """The result names the swap that expired unsent, no swap is among its outcomes,
    nothing was sold and no record holds the wallet."""
    (released,) = result["released_expired"]
    assert (released["operation"], operations(result)) == ("swap", earlier)
    assert "unresolved" not in result and "record_kept" not in result
    assert (ax_actions.kept_records(), setting.balances()) == ([], start)


def test_a_swap_that_expires_unmined_is_released_and_the_result_says_so(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record, transaction = stopped_with_the_swap_unmined(setting, monkeypatch, capsys)
    recovered = setting.recover(record)
    assert (recovered["resolved"], record.exists()) == (True, False)
    assert recovered["released_expired"] == [{"operation": "swap", "transaction": transaction}]
    assert_released(setting, recovered, ["approve"], start)

    # The wallet is free again. A command that waits through the same expiry ends
    # without success and reports the release itself; the approval still stands.
    waited = executed_past_the_deadline(setting, monkeypatch)
    assert (waited["executed"], waited["completed"]) == (True, False)
    assert_released(setting, waited, [], start)


def test_every_declared_cell_has_a_trading_reference_to_rehearse():
    cells = declared_cells()
    found = [reference(name, chain) for name, chain in cells]
    assert {name for name, _ in cells} == set(CONNECTORS)
    assert all(item.amount > 0 and item.asset.chain == item.chain for item in found)
