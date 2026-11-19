"""`almanak ax swap` on deployed Uniswap V4 pools, one process per command.

Every cell that declares the swap as a direct action is rehearsed, and the Ethereum
pool of the native coin beside them: an isolated Anvil fork of the chain stands in for
mainnet behind the production adapter's own planning, signing and reconciliation, and
the command's own resolution builds each deployment. It is rehearsal evidence, not a
mainnet run.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.uniswap_v4 import reader as reads
from almanak.connectors.uniswap_v4.resolution import MarketSelection
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import balance
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import AssetAmount, Chain
from tests.fork import test_ax_swap as family

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
CONNECTOR = "uniswap_v4"
SLIPPAGE_BPS = 100
STEPS = ["approve_erc20", "approve_permit2", "swap"]
# The trading reference of each chain; the Ethereum one carries no chain in its name.
REFERENCES = {
    "eip155:1": "uniswap_v4_trading",
    "eip155:43114": "uniswap_v4_avalanche_trading",
    "eip155:8453": "uniswap_v4_base_trading",
    "eip155:42161": "uniswap_v4_arbitrum_trading",
    "eip155:10": "uniswap_v4_optimism_trading",
    "eip155:56": "uniswap_v4_bnb_trading",
    "eip155:137": "uniswap_v4_polygon_trading",
    "eip155:4663": "uniswap_v4_robinhood_trading",
}
NATIVE_REFERENCE = "uniswap_v4_ethereum_native_trading"
NATIVE_COIN = "eip155:1/slip44:60"
# Above the gas of either approval with its buffer on every chain, below that of a swap.
APPROVALS_ONLY_GAS = 100_000


def declared_chains():
    """Every chain that declares the swap as a direct action."""
    cells = connector_from_wire({"connector": CONNECTOR}).chain_support
    return [str(cell.chain) for cell in cells if cell.direct_actions]


@dataclass(frozen=True)
class Reference:
    """The pinned block, pool key and funded asset of one trading reference."""

    name: str
    chain: ChainId
    key: tuple
    height: int
    block_hash: str
    asset: AssetType
    # Base units of `asset` the wallet starts with; each swap sells a quarter.
    funded: int
    native: int

    @property
    def amount(self) -> int:
        return self.funded // 4

    @property
    def market(self) -> MarketSelection:
        selected = {"connector": CONNECTOR, **dict(self.key)}
        return MarketSelection.model_validate_json(json.dumps(selected))


def reference(name: str) -> Reference:
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    markets = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())["markets"]
    (market,) = markets.values()
    (anchor,) = fork["anchors"]
    (fund,) = fork["funds"]
    (funded,) = fund["assets"]
    (operator,) = fork["operators"]
    key = {name: value for name, value in market.items() if name != "connector"}
    return Reference(
        name=name,
        chain=ChainId(market["chain"]),
        key=tuple(key.items()),
        height=anchor["height"],
        block_hash=anchor["reference"],
        asset=AssetType(funded["asset"]),
        funded=funded["amount"],
        native=operator["native_balance"]["amount"],
    )


def started(request, cell):
    """An isolated fork of the reference's chain at its pinned block."""
    env = request.config.getoption("--fork-env").resolve()
    chain = Chain(chain_id=cell.chain, environment="mainnet")
    with EthereumReader(chain, read_endpoint(cell.chain, env)) as reader:
        origin = reader.pin_block(cell.height)
    assert origin.hash == cell.block_hash
    target = ForkTarget(chain=chain, provider=ax_actions.alchemy(cell.chain), block=origin)
    return Fork(env, origin, target=target)


class Setting:
    """One funded wallet on one fork; each call is one command with its own deployment."""

    def __init__(self, fork, cell, wallet, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        self.owner = wallet["address"].lower()
        self.chain = Chain(chain_id=cell.chain, environment="mainnet")

    def request(self, amount=None, sell=None):
        return ActionRequest(
            "swap",
            self.cell.amount if amount is None else amount,
            sell=str(sell or self.cell.asset),
            slippage_bps=SLIPPAGE_BPS,
            deadline_seconds=300,
        )

    def config(self, amount=None, sell=None, limits=family.LIMITS):
        """The deployment as the command builds it, resolution included."""
        return ax_actions.deployment(
            CONNECTOR,
            self.cell.market,
            self.request(amount, sell),
            self.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )

    def __call__(self, amount=None, *, execute, sell=None, limits=family.LIMITS):
        config = self.config(amount, sell, limits)
        gateway = family.backend(self.fork, config, self.wallet, self.identity)
        try:
            return ax_actions.run(config, gateway, self.request(amount, sell), execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(
            record, lambda stored: family.backend(self.fork, stored, self.wallet, self.identity)
        )

    def target(self):
        return self.config().target(ALLOCATION)

    def bought(self):
        return next(asset for asset in self.target().assets if asset != self.cell.asset)

    def balances(self):
        """The wallet's balances of the sold and the bought token, in base units."""
        return tuple(
            balance(self.fork.rpc, erc20_address(asset, self.cell.chain), self.owner)
            for asset in (self.cell.asset, self.bought())
        )

    def native_balance(self):
        return int(self.fork.rpc("eth_getBalance", [self.owner, "latest"]), 16)

    def authority(self, asset=None):
        """What the chain holds for one token at the head: its ERC-20 allowance to
        Permit2, then the amount and the expiration of its Permit2 allowance to the router."""
        target = self.target()
        asset = asset or self.cell.asset
        with EthereumReader(self.chain, self.fork.rpc.endpoint) as reader:
            block = reader.pin_block("latest")
            token = reads.allowance(reader, target, asset, block)
            amount, expiration, _ = reads.permit_allowance(
                reader, target, asset, target.contracts.router, block
            )
        return token, amount, expiration

    def quoted_output(self):
        """The pool's own quote for the stated amount at the fork's head."""
        with EthereumReader(self.chain, self.fork.rpc.endpoint) as reader:
            stated = AssetAmount(asset=self.cell.asset, amount=self.cell.amount)
            quote = reads.quote(reader, self.target(), stated, reader.pin_block("latest"))
        return quote.output.amount

    def head_time(self):
        return int(self.fork.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)


def prepared(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet = family.funded_wallet(fork, cell, tmp_path / "wallet")
    identity = family.as_mainnet(fork, cell.chain)
    return Setting(fork, cell, wallet, identity)


@pytest.fixture(params=declared_chains(), ids=lambda chain: family.NETWORKS[chain])
def setting(request, tmp_path, monkeypatch):
    cell = reference(REFERENCES[request.param])
    with started(request, cell) as fork:
        yield prepared(fork, cell, tmp_path, monkeypatch)


@pytest.fixture
def native(request, tmp_path, monkeypatch):
    cell = reference(NATIVE_REFERENCE)
    with started(request, cell) as fork:
        yield prepared(fork, cell, tmp_path, monkeypatch)


def left(setting, token, permit, expiration):
    """The allowances of the sold token as a result lists them: nonzero amounts only."""
    cell, contracts = setting.cell, setting.target().contracts
    rows = [{"asset": str(cell.asset), "spender": contracts.permit2, "amount": str(token)}]
    granted = {"asset": str(cell.asset), "spender": contracts.router, "amount": str(permit)}
    rows.append({**granted, "expiration": expiration})
    return [row for row in rows if row["amount"] != "0"]


def assert_preview(setting, preview):
    cell = setting.cell
    step = preview["first_step"]["operation"]
    held = family.inventory(preview, "position_before")
    assert (preview["action"], preview["executed"]) == ("swap", False)
    assert "outcomes" not in preview and "position_after" not in preview
    assert (step["kind"], step["amount"]) == ("approve_erc20", cell.amount)
    assert held[str(cell.asset)] == sum(held.values()) == cell.amount


def assert_swapped(setting, result, before, minimum, steps=STEPS):
    """The wallet moved by exactly the sold amount and by what the result reports bought."""
    cell = setting.cell
    after = setting.balances()
    held = family.inventory(result, "position_after")
    bought = sum(held.values()) - held[str(cell.asset)]
    assert family.operations(result) == steps
    assert "refused" not in result and "unresolved" not in result
    assert (held[str(cell.asset)], before[0] - after[0]) == (0, cell.amount)
    assert after[1] - before[1] == bought >= minimum > 0


def assert_spent(setting, result):
    """A swap spends both allowances whole; the Permit2 record keeps only its expiration."""
    token, amount, expiration = setting.authority()
    assert (result["position_after"]["allowances"], token, amount) == ([], 0, 0)
    assert expiration > 0


def assert_refused_beyond_the_balance(setting):
    """More than the wallet holds is refused before anything is planned."""
    held = setting.balances()[0]
    with pytest.raises(DataError, match="the wallet holds") as refusal:
        setting(held + 1, execute=True)
    assert (refusal.value.kind, setting.balances()[0]) == ("inventory", held)


def test_a_swap_sells_exactly_the_stated_amount_and_spends_its_allowances(setting):
    start = setting.balances()
    preview = setting(execute=False)
    assert_preview(setting, preview)
    # A preview signs nothing: no balance and no allowance moved.
    assert (start, setting.balances()) == ((setting.cell.funded, 0), start)
    assert (preview["position_before"]["allowances"], setting.authority()) == ([], (0, 0, 0))

    minimum = setting.quoted_output() * (10000 - SLIPPAGE_BPS) // 10000
    first = setting(execute=True)
    assert_swapped(setting, first, start, minimum)
    assert_spent(setting, first)

    # A later command is a new deployment: it sells its own stated amount again.
    middle = setting.balances()
    second = setting(execute=True)
    assert_swapped(setting, second, middle, 1)
    assert_spent(setting, second)
    assert first["completed"] and second["completed"]
    assert_refused_beyond_the_balance(setting)


def assert_recovered(setting, recovered, record, start):
    """The signed approval is resolved, the unsigned rest dropped, and nothing was sold."""
    cell = setting.cell
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (family.operations(recovered), "unresolved" in recovered) == (STEPS[:1], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert (setting.balances(), setting.authority()) == (start, (cell.amount, 0, 0))


def test_an_interrupted_swap_is_recovered_and_reports_the_approval_it_left(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record = family.stopped_while_waiting_for("approve_erc20", setting, monkeypatch, capsys)
    family.assert_blocked(setting, record, "approve_erc20")
    recovered = setting.recover(record)
    assert_recovered(setting, recovered, record, start)
    # The ERC-20 approval to Permit2 that was mined stands, and the result says so.
    approval = left(setting, setting.cell.amount, 0, 0)
    assert recovered["position_after"]["allowances"] == approval

    # Restating the same swap spends it: only the Permit2 allowance and the swap remain.
    again = setting(execute=True)
    assert again["position_before"]["allowances"] == approval and again["completed"]
    assert_swapped(setting, again, start, 1, STEPS[1:])
    assert_spent(setting, again)


def test_a_swap_interrupted_after_it_was_sent_is_recovered_from_its_receipt(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record = family.stopped_while_waiting_for("swap", setting, monkeypatch, capsys)
    family.assert_blocked(setting, record, "swap")
    # The request finished with the swap's receipt, so nothing was left to drop.
    recovered = setting.recover(record)
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_swapped(setting, recovered, start, 1)
    assert_spent(setting, recovered)


def refused_after_the_approvals(setting):
    """Run a swap under a gas limit that admits both approvals and refuses the swap."""
    tight = ax_actions.FeeLimits(
        family.LIMITS.max_gas_price_wei,
        family.LIMITS.max_transaction_fee,
        max_gas_limit=APPROVALS_ONLY_GAS,
    )
    return setting(execute=True, limits=tight)


def assert_refused(setting, refused, start):
    """Nothing is signed and unresolved, so no record is kept and the wallet is free."""
    assert (refused["executed"], refused["completed"]) == (True, False)
    assert (family.operations(refused), refused["refused"]["kind"]) == (STEPS[:2], "gas_limit")
    assert "unresolved" not in refused and "record_kept" not in refused
    assert (ax_actions.kept_records(), setting.balances()) == ([], start)


def assert_both_stand(setting, refused):
    """Both approvals stand: the ERC-20 one without limit of time, the Permit2 one until
    the deadline the refused swap would have had."""
    cell = setting.cell
    token, amount, expiration = setting.authority()
    assert (token, amount) == (cell.amount, cell.amount)
    assert setting.head_time() < expiration <= setting.head_time() + 300
    assert refused["position_after"]["allowances"] == left(setting, token, amount, expiration)


def test_a_swap_refused_after_its_approvals_reports_both_and_a_later_swap_uses_them(setting):
    cell = setting.cell
    start = setting.balances()
    refused = refused_after_the_approvals(setting)
    assert_refused(setting, refused, start)
    assert_both_stand(setting, refused)
    expiration = setting.authority()[2]

    # A smaller swap inside that time needs neither approval, and what it does not
    # spend of them remains: the result lists the rest.
    other = cell.amount // 2
    rest = cell.amount - other
    again = setting(other, execute=True)
    assert (family.operations(again), again["completed"]) == (["swap"], True)
    assert start[0] - setting.balances()[0] == other
    assert setting.authority() == (rest, rest, expiration)
    assert again["position_after"]["allowances"] == left(setting, rest, rest, expiration)


def test_a_native_pool_trades_the_wrapped_token_and_moves_no_native_value(native):
    start, coin = native.balances(), native.native_balance()
    sold = native(execute=True)
    assert_swapped(native, sold, start, 1)
    assert_spent(native, sold)

    # The other way, the pool pays the native coin and the wallet receives it wrapped.
    middle = native.balances()
    back = native(middle[1] // 2, sell=native.bought(), execute=True)
    assert (family.operations(back), back["completed"]) == (STEPS, True)
    assert native.balances()[0] > middle[0] and native.authority(native.bought())[:2] == (0, 0)
    # Only gas left the wallet's native balance, far less than either amount traded.
    assert 0 < coin - native.native_balance() < native.cell.amount // 10


def test_the_native_coin_itself_is_refused_as_the_sold_asset(native):
    target = native.target()
    assert (str(target.pool.currency0), target.assets[0]) == (NATIVE_COIN, native.cell.asset)
    with pytest.raises(ValueError, match="sells as its wrapped token"):
        native(sell=NATIVE_COIN, execute=False)
    assert native.balances() == (native.cell.funded, 0)


def test_every_declared_cell_has_a_trading_reference_to_rehearse():
    chains = declared_chains()
    found = [reference(REFERENCES[chain]) for chain in chains]
    assert len(chains) == 8 and [str(item.chain) for item in found] == chains
    assert all(item.amount > 0 and item.asset.chain == item.chain for item in found)
