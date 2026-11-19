"""`almanak ax swap` on deployed Curve pools, one process per command.

Every chain that declares the swap as a direct action is rehearsed in every pool family
it declares: an isolated Anvil fork of the chain stands in for mainnet behind the
production adapter's own planning, signing and reconciliation, and the command's own
resolution builds each deployment. The fork keeps the time of its pinned block. It is
rehearsal evidence, not a mainnet run.
"""

import json
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path

import pytest
import yaml

from almanak import ax_actions
from almanak import state as durable
from almanak.caip import AssetType, ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import observations
from almanak.connectors.curve.reader import quantities
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import (
    balance,
    fund_erc20,
    read_uint,
    send_as,
    transfer_data,
)
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import AssetAmount, Chain
from almanak.transaction_family import is_unsent
from tests.fork.test_ax_swap import (
    LIMITS,
    NETWORKS,
    assert_blocked,
    backend,
    funded_wallet,
    inventory,
    operations,
    stopped_while_waiting_for,
)

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
SLIPPAGE_BPS = 100
# Seconds after the observed block within which each swap may be signed.
SIGNING_WINDOW = 300
RECEIPT = "eth_getTransactionReceipt"
# The pool family part of each liquidity reference's name, per declared variant.
FAMILIES = {
    "stableswap_ng": "stable",
    "twocrypto_ng": "twocrypto",
    "tricrypto_ng": "tricrypto",
    "tricrypto_ng_2024": "tricrypto",
}


def declared_cells():
    """Every chain that declares the swap as a direct action, in each variant it declares."""
    return [
        (str(support.chain), str(variant))
        for support in connector_from_wire({"connector": "curve"}).chain_support
        if support.direct_actions
        for variant in support.variants
    ]


@dataclass(frozen=True)
class Reference:
    """The pinned block, pool, pair and funded coin of a cell's liquidity reference."""

    connector: str
    chain: ChainId
    variant: str
    pool: str
    height: int
    block_hash: str
    # The coin sold, which the reference funds, and the coin bought.
    asset: AssetType
    bought: AssetType
    # Base units of `asset` the wallet starts with; each swap sells a quarter.
    funded: int
    native: int

    @property
    def amount(self) -> int:
        return self.funded // 4


def reference(chain: str, variant: str) -> Reference:
    name = f"curve_{NETWORKS[chain]}_{FAMILIES[variant]}_lp"
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    config = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    (market,) = config["markets"].values()
    allocation = next(iter(config["strategy"]["allocations"].values()))
    (anchor,) = fork["anchors"]
    (funded,) = fork["funds"][0]["assets"]
    (operator,) = fork["operators"]
    assert funded["asset"] == allocation["funding_asset"]
    return Reference(
        connector="curve",
        chain=ChainId(chain),
        variant=variant,
        pool=market["pool"],
        height=anchor["height"],
        block_hash=anchor["reference"],
        asset=AssetType(allocation["funding_asset"]),
        bought=AssetType(allocation["trading_asset"]),
        funded=funded["amount"],
        native=operator["native_balance"]["amount"],
    )


@pytest.fixture(params=declared_cells(), ids=lambda cell: f"{NETWORKS[cell[0]]}-{cell[1]}")
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


def as_mainnet(fork, chain):
    """Answer as the forked chain, at the fork's own time."""
    fork.rpc("anvil_setChainId", [chain_number(chain)])
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


class Setting:
    """One funded wallet on one fork; each call is one command with its own deployment."""

    def __init__(self, fork, cell, wallet, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        selection = ax_actions.resolution("curve").MarketSelection
        self.market = selection(connector="curve", chain=cell.chain, pool=cell.pool)
        self.owner = wallet["address"].lower()

    def request(self, amount=None):
        return ActionRequest(
            "swap",
            self.cell.amount if amount is None else amount,
            sell=str(self.cell.asset),
            buy=str(self.cell.bought),
            slippage_bps=SLIPPAGE_BPS,
            deadline_seconds=SIGNING_WINDOW,
        )

    def config(self, amount=None, limits=LIMITS):
        """The deployment as the command builds it, resolution included."""
        return ax_actions.deployment(
            "curve",
            self.market,
            self.request(amount),
            self.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )

    def __call__(self, amount=None, *, execute, limits=LIMITS):
        return self.run(self.config(amount, limits), amount, execute=execute)

    def run(self, config, amount=None, *, execute):
        """Run a deployment the command already resolved."""
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

    def held(self, asset):
        return balance(self.fork.rpc, erc20_address(asset, self.cell.chain), self.owner)

    def balances(self):
        """The wallet's balances of the sold and the bought coin, in base units."""
        return self.held(self.cell.asset), self.held(self.cell.bought)

    def untouched(self):
        """The wallet's balances of the pool's other coins and of its LP token."""
        pool = self.target().pool
        others = [coin for coin in pool.coins if coin not in (self.cell.asset, self.cell.bought)]
        return tuple(self.held(asset) for asset in (*others, pool.shares_asset))

    def pool_allowance(self):
        token = erc20_address(self.cell.asset, self.cell.chain)
        return read_uint(
            self.fork.rpc, token, "allowance(address,address)", (self.owner, self.cell.pool)
        )

    def pool_holds(self):
        """The pool's own accounting of the sold coin at the fork's head, in base units."""
        pool = self.target().pool
        chain = Chain(chain_id=self.cell.chain, environment="mainnet")
        with EthereumReader(chain, self.fork.rpc.endpoint) as reader:
            held = quantities(reader, pool, reader.pin_block("latest"), "balances")
        return held[pool.coins.index(self.cell.asset)]

    def quoted_output(self):
        """The pool's own quote for the stated amount at the fork's head."""
        target = self.target()
        chain = Chain(chain_id=self.cell.chain, environment="mainnet")
        with EthereumReader(chain, self.fork.rpc.endpoint) as reader:
            quote = observations.measured_quote(
                reader,
                target,
                reader.pin_block("latest"),
                AssetAmount(asset=self.cell.asset, amount=self.cell.amount),
                self.cell.bought,
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


def left_to_pool(setting):
    """The allowance an approval of the stated amount leaves, as a result reports it."""
    cell = setting.cell
    return [{"asset": str(cell.asset), "spender": cell.pool, "amount": str(cell.amount)}]


def assert_preview(setting, preview):
    cell = setting.cell
    step = preview["first_step"]["operation"]
    held = inventory(preview, "position_before")
    coins = list(map(str, setting.target().pool.coins))
    assert (preview["action"], preview["executed"]) == ("swap", False)
    assert not {"outcomes", "position_after"} & set(preview)
    assert (step["kind"], step["spender"], step["amount"]) == ("approve", cell.pool, cell.amount)
    # Every coin of the pool is listed, and only the sold one is attributed.
    assert (list(held), held[str(cell.asset)], sum(held.values())) == (
        coins,
        cell.amount,
        cell.amount,
    )


def assert_swapped(setting, result, before, minimum):
    """The wallet moved by exactly the sold amount and by what the result reports bought."""
    cell = setting.cell
    after = setting.balances()
    held = inventory(result, "position_after")
    bought = held[str(cell.bought)]
    assert operations(result) == ["approve", "swap"]
    assert "refused" not in result and "unresolved" not in result
    assert (held[str(cell.asset)], before[0] - after[0]) == (0, cell.amount)
    assert after[1] - before[1] == bought == sum(held.values()) >= minimum > 0


def assert_no_allowance(setting, result):
    assert (result["position_after"]["allowances"], setting.pool_allowance()) == ([], 0)


def assert_refused_beyond_the_balance(setting):
    """More than the wallet holds is refused before anything is planned."""
    held = setting.balances()[0]
    with pytest.raises(DataError, match="the wallet holds") as refusal:
        setting(held + 1, execute=True)
    assert (refusal.value.kind, setting.balances()[0]) == ("inventory", held)


def test_the_resolved_pool_is_the_declared_variant(setting):
    pool = setting.target().pool
    assert str(pool.variant) == setting.cell.variant
    assert {setting.cell.asset, setting.cell.bought} <= set(pool.coins)


def test_a_swap_sells_exactly_the_stated_amount_and_leaves_no_allowance(setting):
    start, aside = setting.balances(), setting.untouched()
    preview = setting(execute=False)
    assert_preview(setting, preview)
    # A preview signs nothing: no balance and no allowance moved.
    assert (start, setting.balances()) == ((setting.cell.funded, 0), start)
    assert (preview["position_before"]["allowances"], setting.pool_allowance()) == ([], 0)

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
    # The pool's other coins and its LP token never moved.
    assert setting.untouched() == aside and aside[-1] == 0
    assert_refused_beyond_the_balance(setting)


def assert_recovered(setting, recovered, record, start):
    """The signed approval is resolved, the unsigned swap dropped, and nothing was sold."""
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (operations(recovered), "unresolved" in recovered) == (["approve"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert (setting.balances(), setting.pool_allowance()) == (start, setting.cell.amount)


def test_an_interrupted_swap_is_recovered_and_reports_the_allowance_it_left(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record = stopped_while_waiting_for("approve", setting, monkeypatch, capsys)
    assert_blocked(setting, record, "approve")
    recovered = setting.recover(record)
    assert_recovered(setting, recovered, record, start)
    # The approval that was mined stands, and the result says so.
    assert recovered["position_after"]["allowances"] == left_to_pool(setting)

    # Restating the same swap needs only its own step, and the allowance is used up.
    again = setting(execute=True)
    assert (operations(again), again["completed"]) == (["swap"], True)
    assert again["position_before"]["allowances"] == left_to_pool(setting)
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


def swap_sent(pending) -> bool:
    return pending is not None and pending.operation.kind == "swap" and not is_unsent(pending)


def holding_the_swap(fork):
    """A wait that stops the node mining once the approval is in, and stops the command
    once its swap has been sent."""

    def wait(self, state):
        if swap_sent(state.pending):
            raise KeyboardInterrupt
        if state.outcomes:
            fork.rpc("evm_setAutomine", [False])

    return wait


def kept_record(capsys):
    """The record an interrupted command reported that it kept."""
    lines = capsys.readouterr().err.splitlines()
    events = [json.loads(line) for line in lines if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def dropped(fork, sent):
    """Take a sent, unmined transaction out of the node, which then mines again."""
    known = [fork.rpc(method, [sent]) for method in ("eth_getTransactionByHash", RECEIPT)]
    assert known[0] is not None and known[1] is None
    fork.rpc("anvil_dropTransaction", [sent])
    assert fork.rpc("eth_getTransactionByHash", [sent]) is None
    fork.rpc("evm_setAutomine", [True])


def sent_but_never_mined(setting, monkeypatch, capsys):
    """Run a swap whose approval is mined and whose own transaction is sent to a node
    that mines nothing, then drop that transaction from the node."""
    from almanak.operator_runtime import OperatorRuntime

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", holding_the_swap(setting.fork))
        with pytest.raises(KeyboardInterrupt):
            setting(execute=True)
    record = kept_record(capsys)
    (listed,) = ax_actions.kept_records()
    sent = listed["unresolved"]["transaction"]
    dropped(setting.fork, sent)
    return record, sent


def an_hour_passes(fork):
    """Twelve times the window in which the swap could be signed."""
    fork.rpc("evm_increaseTime", [12 * SIGNING_WINDOW])
    fork.rpc("evm_mine")


def test_a_signed_swap_that_was_not_mined_is_sent_again_long_after_its_signing_window(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record, sent = sent_but_never_mined(setting, monkeypatch, capsys)
    assert (setting.balances(), setting.pool_allowance()) == (start, setting.cell.amount)
    an_hour_passes(setting.fork)

    # No Curve call carries a deadline, so the same signed bytes are sent and mined.
    recovered = setting.recover(record)
    mined = setting.fork.rpc(RECEIPT, [sent])
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert (recovered["outcomes"][-1]["transaction"], mined["status"]) == (sent, "0x1")
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_swapped(setting, recovered, start, 1)
    assert_no_allowance(setting, recovered)


def moved_against(setting, directory):
    """Sell the pool as much of the sold coin as it holds, from another wallet."""
    cell = setting.cell
    size = setting.pool_holds()
    wallet = funded_wallet(setting.fork, replace(cell, funded=size), directory)
    mover = Setting(setting.fork, cell, wallet, setting.identity)
    assert mover(size, execute=True)["completed"]


def assert_unsendable(setting, record, sent):
    """Recovery sends nothing and resolves nothing; it reports why."""
    refused = setting.recover(record)
    assert (refused["resolved"], refused["discarded_request"]) == (False, False)
    assert refused["refused"]["kind"] == "signed_attempt_unsendable"
    assert refused["unresolved"] == {"operation": "swap", "transaction": sent}
    assert setting.fork.rpc("eth_getTransactionByHash", [sent]) is None
    return refused


# One chain's three pool families: the refusal is the gateway's own, whatever the chain.
MOVED = [("eip155:1", variant) for variant in ("stableswap_ng", "twocrypto_ng", "tricrypto_ng")]


@pytest.mark.parametrize("cell", MOVED, indirect=True, ids=[variant for _, variant in MOVED])
def test_a_signed_swap_the_market_moved_away_from_is_kept_and_never_expires(
    setting, monkeypatch, capsys, tmp_path
):
    start = setting.balances()
    record, sent = sent_but_never_mined(setting, monkeypatch, capsys)
    signed = durable.Journal(record / "state.sqlite").load().pending.operation
    moved_against(setting, tmp_path / "mover")
    assert setting.quoted_output() < signed.minimum.amount

    # The pool would now pay less than the signed minimum, so the bytes are not sent,
    # and the kept record still blocks the wallet.
    assert_unsendable(setting, record, sent)
    assert_blocked(setting, record, "swap")
    # Nothing retires the signed bytes either: an hour later the answer is the same.
    an_hour_passes(setting.fork)
    refused = assert_unsendable(setting, record, sent)
    # The wallet sold nothing, and the approval the swap would spend still stands.
    assert (setting.balances(), setting.pool_allowance()) == (start, setting.cell.amount)
    assert refused["position_after"]["allowances"] == left_to_pool(setting)


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
    assert refused["position_after"]["allowances"] == left_to_pool(setting)
    assert setting.pool_allowance() == setting.cell.amount

    # A swap of another amount resets the allowance that remained before its own.
    other = setting.cell.amount // 2
    again = setting(other, execute=True)
    assert (operations(again), again["completed"]) == (["approve", "approve", "swap"], True)
    assert start[0] - setting.balances()[0] == other
    assert_no_allowance(setting, again)


def assert_refused_for_shares(command, shares, held):
    """The command is refused for the LP tokens the wallet holds, naming them."""
    with pytest.raises(DataError, match=f"holds {held} base units") as refusal:
        command()
    assert (refusal.value.kind, str(shares) in str(refusal.value)) == ("ownership", True)


def test_a_wallet_that_holds_the_pools_lp_token_is_refused_before_anything_is_planned(setting):
    start = setting.balances()
    resolved = setting.config()
    shares = resolved.target(ALLOCATION).pool.shares_asset
    fund_erc20(setting.fork, setting.owner, shares, 10**15)
    # A deployment resolved before the tokens arrived is refused at its first
    # observation; a command stated after they arrived, at its resolution.
    late = partial(setting.run, resolved, execute=True)
    for command in (late, partial(setting, execute=False), partial(setting, execute=True)):
        assert_refused_for_shares(command, shares, 10**15)
    # Nothing was planned or signed: no record, no allowance, and every balance as it was.
    assert (ax_actions.kept_records(), setting.pool_allowance()) == ([], 0)
    assert (setting.balances(), setting.held(shares)) == (start, 10**15)


# An account of no one's that sends the wallet LP tokens it did not ask for.
SENDER = "0x" + "5e" * 20


def sending_shares_once_signed(setting, shares):
    """A wait that sends the wallet one base unit of the pool's LP token as soon as its
    swap is signed, and stops the command before the swap is sent."""
    token = erc20_address(shares, setting.cell.chain)

    def wait(self, state):
        if state.pending is not None and state.pending.operation.kind == "swap":
            send_as(setting.fork, SENDER, token, transfer_data(setting.owner, 1))
            raise KeyboardInterrupt

    return wait


def signed_then_sent_shares(setting, monkeypatch, capsys):
    """Run a swap that is stopped once signed, with LP tokens arriving at that moment."""
    from almanak.operator_runtime import OperatorRuntime

    shares = setting.target().pool.shares_asset
    fund_erc20(setting.fork, SENDER, shares, 10)
    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", sending_shares_once_signed(setting, shares))
        with pytest.raises(KeyboardInterrupt):
            setting(execute=True)
    return kept_record(capsys), shares


def test_lp_tokens_sent_after_the_swap_is_signed_are_reported_and_the_record_resolves(
    setting, monkeypatch, capsys
):
    start = setting.balances()
    record, shares = signed_then_sent_shares(setting, monkeypatch, capsys)
    signed = durable.Journal(record / "state.sqlite").load().pending
    assert (signed.operation.kind, setting.held(shares), setting.balances()) == ("swap", 1, start)

    # The signed swap is sent, mined and reconciled from its own effects. The tokens no
    # step explains are listed on its outcome, and the record is gone.
    recovered = setting.recover(record)
    issues = recovered["outcomes"][-1]["accounting_issues"]
    assert (recovered["resolved"], len(issues), str(shares) in issues[0]) == (True, 1, True)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_swapped(setting, recovered, start, 1)
    assert_no_allowance(setting, recovered)
    # The wallet still holds them, so its next swap is refused before anything is signed.
    assert_refused_for_shares(partial(setting, execute=True), shares, 1)


def test_every_declared_cell_has_a_reference_and_every_variant_is_rehearsed():
    cells = declared_cells()
    found = [reference(chain, variant) for chain, variant in cells]
    declared = connector_from_wire({"connector": "curve"}).chain_support
    assert {chain for chain, _ in cells} == {str(s.chain) for s in declared if s.direct_actions}
    assert {variant for _, variant in cells} == set(FAMILIES)
    assert all(
        item.amount > 0 and item.asset.chain == item.bought.chain == item.chain for item in found
    )
