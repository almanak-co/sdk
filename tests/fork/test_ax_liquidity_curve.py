"""The direct liquidity commands of `almanak ax` on deployed Curve pools.

Every chain that declares the liquidity actions as direct actions is rehearsed in every
pool family it declares, each step as its own command with its own deployment: an
isolated Anvil fork of the chain stands in for mainnet behind the production adapter's
own planning, signing and reconciliation, and the command's own resolution builds each
deployment. The fork keeps the time of its pinned block. It is rehearsal evidence, not a
mainnet run.
"""

from pathlib import Path

import pytest
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.asset_amounts import AssetAmounts
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import abi
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve.position_request import PositionRequest, read_liquidity_position
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
from almanak.gateway.safe_fixture import calldata
from almanak.models import AssetAmount, Chain
from tests.fork.test_ax_liquidity import LIMITS, assert_clean, funded_wallet, operations
from tests.fork.test_ax_swap import backend
from tests.fork.test_ax_swap_curve import (
    FAMILIES,
    NETWORKS,
    SENDER,
    SIGNING_WINDOW,
    as_mainnet,
    kept_record,
    reference,
)

pytestmark = pytest.mark.fork
SLIPPAGE_BPS = 100
FAR_DEADLINE = 2**40


def declared_cells():
    """Every chain that declares opening liquidity as a direct action, in each variant."""
    return [
        (str(support.chain), str(variant))
        for support in connector_from_wire({"connector": "curve"}).chain_support
        if "open_liquidity" in {str(action) for action in support.direct_actions}
        for variant in support.variants
    ]


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


def stated(amounts) -> tuple:
    """A deposit's amounts as the command line states them: one pair per named coin."""
    return tuple((str(coin), amount) for coin, amount in amounts.items())


class Setting:
    """One funded wallet on one fork; each call is one command with its own deployment."""

    def __init__(self, fork, cell, wallet, signer, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        self.signer = signer
        self.owner = wallet["address"].lower()
        selection = ax_actions.resolution("curve").MarketSelection
        self.market = selection(connector="curve", chain=cell.chain, pool=cell.pool)
        # The liquidity target the commands build, for calls made outside them.
        self.target = self.config("revoke-exchange").target(ALLOCATION)
        self.pool = self.target.pool
        self.assets = (*self.pool.coins, self.pool.shares_asset)

    def reader(self):
        chain = Chain(chain_id=self.cell.chain, environment="mainnet")
        return EthereumReader(chain, self.fork.rpc.endpoint)

    def request(self, action, **named):
        priced = action != "revoke-exchange"
        return ActionRequest(
            action,
            slippage_bps=SLIPPAGE_BPS if priced else None,
            deadline_seconds=SIGNING_WINDOW if priced else None,
            **named,
        )

    def config(self, action, limits=LIMITS, **named):
        """The deployment as the command builds it, resolution included."""
        return ax_actions.deployment(
            "curve",
            self.market,
            self.request(action, **named),
            self.wallet,
            ax_actions.alchemy(self.cell.chain),
            self.fork.rpc.endpoint,
            limits,
        )

    def __call__(self, action, *, execute=True, **named):
        config = self.config(action, **named)
        gateway = backend(self.fork, config, self.wallet, self.identity)
        try:
            return ax_actions.run(config, gateway, self.request(action, **named), execute=execute)
        finally:
            gateway.close()

    def recover(self, record):
        return ax_actions.recover(
            record, lambda stored: backend(self.fork, stored, self.wallet, self.identity)
        )

    def held(self, asset):
        return balance(self.fork.rpc, erc20_address(asset, self.cell.chain), self.owner)

    def balances(self):
        """The wallet's balance of every pool coin, then of the pool's LP token."""
        return [self.held(asset) for asset in self.assets]

    def allowances(self):
        """What the wallet lets the pool spend of each coin, nonzero only."""
        found = {}
        for coin in self.pool.coins:
            token = erc20_address(coin, self.cell.chain)
            signature, pair = "allowance(address,address)", (self.owner, self.cell.pool)
            found[(str(coin), self.cell.pool)] = read_uint(self.fork.rpc, token, signature, pair)
        return {key: amount for key, amount in found.items() if amount}

    def send(self, to, data):
        """One transaction signed by the wallet and sent outside `ax`."""
        rpc, sender = self.fork.rpc, self.signer.address
        call = {"from": sender, "to": to_checksum_address(to), "data": data}
        signed = self.signer.sign_transaction(
            {
                "chainId": chain_number(self.cell.chain),
                "nonce": int(rpc("eth_getTransactionCount", [sender, "pending"]), 16),
                "to": call["to"],
                "data": data,
                "value": 0,
                "gas": int(rpc("eth_estimateGas", [call]), 16) * 3 // 2,
                "gasPrice": int(rpc("eth_gasPrice"), 16),
            }
        )
        return rpc.wait_receipt(
            rpc("eth_sendRawTransaction", ["0x" + signed.raw_transaction.hex()])
        )

    def deposit_outside(self, amounts):
        """Approve the pool and add liquidity straight against it, outside `ax`.

        Returns the LP shares the wallet received.
        """
        before = self.held(self.pool.shares_asset)
        for coin, amount in amounts.items():
            approval = ("approve(address,uint256)", ("address", "uint256"))
            token = erc20_address(coin, self.cell.chain)
            self.send(token, calldata(*approval, (self.cell.pool, amount)))
        self.send(self.cell.pool, abi.call(self.deposit(amounts)).data)
        return self.held(self.pool.shares_asset) - before

    def deposit(self, amounts):
        """The pool's own `add_liquidity` for these amounts, minting to the wallet."""
        coins = self.pool.coins
        return ops.Mint(
            target=self.target,
            amounts=AssetAmounts(
                tuple(AssetAmount(asset=coin, amount=amounts.get(coin, 0)) for coin in coins)
            ),
            minimum_shares=AssetAmount(asset=self.pool.shares_asset, amount=1),
            deadline=FAR_DEADLINE,
        )

    def read(self):
        """`ax liquidity-position` for the wallet, at the fork's head."""
        request = PositionRequest(chain=self.cell.chain, pool=self.cell.pool, account=self.owner)
        with self.reader() as reader:
            return read_liquidity_position(reader, request, reader.pin_block("latest"))

    def quarter(self):
        """A quarter of the wallet's funding, in the coin the reference funds."""
        return {self.cell.asset: self.cell.funded // 4}


@pytest.fixture
def setting(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet, signer = funded_wallet(fork, cell, tmp_path / "wallet", cell.funded)
    identity = as_mainnet(fork, cell.chain)
    return Setting(fork, cell, wallet, signer, identity)


def shares(result, moment="position_after"):
    return int(result[moment]["shares"]["amount"])


def amounts_of(result, field, moment="position_after"):
    return [int(item["amount"]) for item in result[moment][field]]


def reported_allowances(result):
    return {
        (item["asset"], item["spender"]): int(item["amount"])
        for item in result["position_after"]["allowances"]
    }


def assert_moved(setting, result, start):
    """The wallet moved by exactly what the result reports, in every coin and in the LP
    token, and its allowances are as reported."""
    moved = amounts_of(result, "moved")
    gained = [new - old for new, old in zip(setting.balances(), start, strict=True)]
    listed = [item["asset"] for item in result["position_after"]["moved"]]
    assert (gained, listed) == (moved, [str(asset) for asset in setting.assets])
    assert reported_allowances(result) == setting.allowances()
    return moved


def swap_for_the_other_coin(setting):
    """Sell a quarter of the funding for the pool's other coin, with `ax swap`."""
    cell = setting.cell
    pair = {"sell": str(cell.asset), "buy": str(cell.bought)}
    swapped = setting("swap", amount=cell.funded // 4, **pair)
    assert_clean(swapped, ["approve", "swap"])
    held = dict(zip(setting.pool.coins, amounts_of(swapped, "inventory"), strict=True))
    return {**setting.quarter(), cell.bought: held[cell.bought]}


def assert_previewed(setting, amounts):
    """A preview plans the first approval to the pool and signs nothing."""
    start = setting.balances()
    preview = setting("open-liquidity", execute=False, amounts=stated(amounts))
    step = preview["first_step"]["operation"]
    first = min(amounts, key=setting.pool.coins.index)
    pool = setting.cell.pool
    assert (preview["executed"], step["kind"], step["spender"]) == (False, "approve", pool)
    assert (step["asset"], step["amount"]) == (str(first), amounts[first])
    assert (setting.balances(), setting.allowances(), shares(preview, "position_before")) == (
        start,
        {},
        0,
    )


def assert_deposited(setting, result, moved, amounts, held):
    """The pool took exactly the stated amounts, minted shares for them, and holds no
    allowance."""
    used = [-amounts.get(coin, 0) for coin in setting.pool.coins]
    assert (moved[:-1], moved[-1] > 0) == (used, True)
    assert shares(result) == held + moved[-1] == setting.balances()[-1]
    assert (any(amounts_of(result, "inventory")), setting.allowances()) == (False, {})


def open_position(setting, amounts):
    assert_previewed(setting, amounts)
    start = setting.balances()
    opened = setting("open-liquidity", amounts=stated(amounts))
    assert_clean(opened, [*["approve"] * len(amounts), "mint"])
    moved = assert_moved(setting, opened, start)
    assert_deposited(setting, opened, moved, amounts, 0)
    return moved[-1]


def assert_read(setting, held):
    """The read reports the wallet's LP balance and what a balanced burn would pay."""
    report = setting.read()
    principal = [int(item["amount"]) for item in report["principal"]]
    assert (report["owner"], int(report["position"]["shares"]["amount"])) == (setting.owner, held)
    assert [item["asset"] for item in report["principal"]] == [str(c) for c in setting.pool.coins]
    assert min(principal) >= 0 < sum(principal)
    return principal


def increase_position(setting, held):
    """Add one coin alone to the shares the wallet holds."""
    amounts, start = setting.quarter(), setting.balances()
    increased = setting("increase-liquidity", amounts=stated(amounts))
    assert_clean(increased, ["approve", "increase"])
    moved = assert_moved(setting, increased, start)
    assert shares(increased, "position_before") == held
    assert_deposited(setting, increased, moved, amounts, held)
    return held + moved[-1]


def decrease_position(setting, held):
    """Burn a third of the shares for every coin in the pool's proportions."""
    burned, start = held // 3, setting.balances()
    decreased = setting("decrease-liquidity", liquidity=burned, receive="balanced")
    assert_clean(decreased, ["decrease"])
    moved = assert_moved(setting, decreased, start)
    assert (moved[-1], min(moved[:-1]) >= 0 < sum(moved[:-1])) == (-burned, True)
    assert shares(decreased) == held - burned == setting.balances()[-1]
    return held - burned


def close_position(setting, held, receive):
    """Burn every share, paid as stated; returns what each coin paid."""
    start = setting.balances()
    closed = setting("close-liquidity", receive=receive)
    assert_clean(closed, ["close"])
    moved = assert_moved(setting, closed, start)
    assert (moved[-1], shares(closed, "position_before")) == (-held, held)
    assert (shares(closed), setting.balances()[-1], setting.allowances()) == (0, 0, {})
    return dict(zip(setting.pool.coins, moved[:-1], strict=True))


def test_liquidity_is_opened_read_increased_decreased_and_closed_each_by_its_own_command(setting):
    amounts = swap_for_the_other_coin(setting)
    held = open_position(setting, amounts)
    principal = assert_read(setting, held)
    held = increase_position(setting, held)
    held = decrease_position(setting, held)
    # A burn paid in one coin pays that coin alone.
    paid = close_position(setting, held, str(setting.cell.asset))
    assert [coin for coin, amount in paid.items() if amount] == [setting.cell.asset]
    assert (sum(principal) > 0, ax_actions.kept_records()) == (True, [])


def assert_one_balance_two_modes(setting, held):
    """The same LP balance is the position of a liquidity command and is not owned by a
    swap, and each command sees only its own reading of it."""
    cell = setting.cell
    with pytest.raises(ValueError, match=f"already holds {held} base units"):
        setting("open-liquidity", amounts=stated(setting.quarter()))
    with pytest.raises(DataError, match=f"holds {held} base units") as refusal:
        setting("swap", amount=cell.funded // 4, sell=str(cell.asset), buy=str(cell.bought))
    preview = setting("decrease-liquidity", execute=False, liquidity=held // 3, receive="balanced")
    step = preview["first_step"]["operation"]
    assert (refusal.value.kind, shares(preview, "position_before")) == ("ownership", held)
    assert (step["kind"], step["shares"]["amount"]) == ("decrease", held // 3)


def test_shares_minted_outside_the_commands_are_adopted_decreased_and_closed(setting):
    start = setting.balances()
    amounts = {setting.cell.asset: setting.cell.funded // 2}
    held = setting.deposit_outside(amounts)
    assert (held > 0, setting.allowances(), assert_read(setting, held) != []) == (True, {}, True)
    assert_one_balance_two_modes(setting, held)

    held = decrease_position(setting, held)
    paid = close_position(setting, held, "balanced")
    # Every share is burned, and the wallet holds each coin the burns paid.
    assert min(paid.values()) >= 0 < sum(paid.values())
    assert (setting.balances()[-1], start[-1], ax_actions.kept_records()) == (0, 0, [])


def stopped_once_signed(kind, setting, monkeypatch, capsys, action, **named):
    """Run a command that is interrupted as soon as its step of this kind is signed."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None and state.pending.operation.kind == kind:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            setting(action, **named)
    return kept_record(capsys)


def assert_blocked(setting, record, kind):
    (kept,) = ax_actions.kept_records()
    assert (kept["path"], kept["unresolved"]["operation"]) == (str(record), kind)
    # The kept record blocks the wallet until its signed step is resolved.
    with pytest.raises(RuntimeError, match="already claimed"):
        setting("revoke-exchange", execute=False)


def revoke_what_is_left(setting, left, start):
    """`revoke-exchange` zeroes the allowance an unfinished deposit left, and only that."""
    assert (setting.allowances(), setting.balances()) == (left, start)
    revoked = setting("revoke-exchange")
    assert_clean(revoked, ["approve"] * len(left))
    assert (reported_allowances(revoked), setting.allowances()) == ({}, {})
    with pytest.raises(ValueError, match="no allowance in any of its coins"):
        setting("revoke-exchange")
    assert setting.balances() == start


def test_an_open_interrupted_at_its_approval_is_recovered_and_its_allowance_revoked(
    setting, monkeypatch, capsys
):
    amounts, start = setting.quarter(), setting.balances()
    named = {"amounts": stated(amounts)}
    record = stopped_once_signed("approve", setting, monkeypatch, capsys, "open-liquidity", **named)
    assert_blocked(setting, record, "approve")
    recovered = setting.recover(record)
    left = {(str(setting.cell.asset), setting.cell.pool): amounts[setting.cell.asset]}
    # The signed approval is resolved and the unsigned deposit dropped; nothing was deposited.
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (operations(recovered), reported_allowances(recovered)) == (["approve"], left)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    revoke_what_is_left(setting, left, start)


def test_an_open_interrupted_once_its_deposit_is_signed_is_recovered(setting, monkeypatch, capsys):
    amounts, start = setting.quarter(), setting.balances()
    named = {"amounts": stated(amounts)}
    record = stopped_once_signed("mint", setting, monkeypatch, capsys, "open-liquidity", **named)
    assert_blocked(setting, record, "mint")
    recovered = setting.recover(record)
    moved = assert_moved(setting, recovered, start)
    # The request finished with the deposit's receipt, so nothing was left to drop.
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, False)
    assert (operations(recovered), "accounting_issues" in recovered["outcomes"][-1]) == (
        ["approve", "mint"],
        False,
    )
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert_deposited(setting, recovered, moved, amounts, 0)


def assert_refused_without_shares(setting):
    """A wallet that holds none of the pool's LP token has nothing to add to or burn."""
    refused = {
        "increase-liquidity": {"amounts": stated(setting.quarter())},
        "decrease-liquidity": {"liquidity": 1, "receive": "balanced"},
        "close-liquidity": {"receive": "balanced"},
    }
    for action, named in refused.items():
        with pytest.raises(ValueError, match="holds none of this pool's LP token"):
            setting(action, **named)
    with pytest.raises(ValueError, match="no allowance in any of its coins"):
        setting("revoke-exchange")


def assert_deposit_refused(setting):
    """More than the wallet holds, and a coin of another pool, are refused before a step."""
    cell = setting.cell
    with pytest.raises(DataError, match="the wallet holds") as short:
        setting("open-liquidity", amounts=((str(cell.asset), cell.funded + 1),))
    foreign = f"{cell.chain}/erc20:0x" + "ab" * 20
    with pytest.raises(ValueError, match="not a coin of the pool"):
        setting("open-liquidity", amounts=((foreign, 1),))
    assert short.value.kind == "inventory"


def assert_burn_refused(setting, held):
    """All of the balance or more is not a decrease, and a burn pays in the pool's coins."""
    foreign = f"{setting.cell.chain}/erc20:0x" + "ab" * 20
    for burned in (held, held + 1):
        with pytest.raises(ValueError, match=f"must be below the wallet's {held} base units"):
            setting("decrease-liquidity", liquidity=burned, receive="balanced")
    with pytest.raises(ValueError, match="--receive must be `balanced` or one of the pool's"):
        setting("close-liquidity", receive=foreign)


def test_what_the_wallet_cannot_cover_or_does_not_hold_is_refused_before_anything_is_signed(
    setting,
):
    start = setting.balances()
    assert_refused_without_shares(setting)
    assert_deposit_refused(setting)
    assert (setting.balances(), setting.allowances(), ax_actions.kept_records()) == (start, {}, [])
    held = setting.deposit_outside(setting.quarter())
    funded = setting.balances()
    assert_burn_refused(setting, held)
    assert (setting.balances(), setting.allowances(), ax_actions.kept_records()) == (funded, {}, [])


def assert_settled(record):
    """No record is kept, so the wallet is free for its next command."""
    assert (ax_actions.kept_records(), record.exists()) == ([], False)


def sending_a_share_once_signed(setting):
    """A wait that sends the wallet one base unit of the pool's LP token as soon as its
    burn is signed, and stops the command before the burn is sent."""
    token = erc20_address(setting.pool.shares_asset, setting.cell.chain)

    def wait(self, state):
        if state.pending is not None and state.pending.operation.kind == "decrease":
            send_as(setting.fork, SENDER, token, transfer_data(setting.owner, 1))
            raise KeyboardInterrupt

    return wait


def burn_signed_then_sent_a_share(setting, monkeypatch, capsys, burned):
    from almanak.operator_runtime import OperatorRuntime

    fund_erc20(setting.fork, SENDER, setting.pool.shares_asset, 10)
    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", sending_a_share_once_signed(setting))
        with pytest.raises(KeyboardInterrupt):
            setting("decrease-liquidity", liquidity=burned, receive="balanced")
    return kept_record(capsys)


def test_an_lp_token_sent_after_a_burn_is_signed_is_reported_and_the_next_command_adopts_it(
    setting, monkeypatch, capsys
):
    held = setting.deposit_outside(setting.quarter())
    burned = held // 3
    record = burn_signed_then_sent_a_share(setting, monkeypatch, capsys, burned)
    assert (setting.balances()[-1], Path(record).exists()) == (held + 1, True)

    # The burn is sent, mined and reconciled from its own receipt. The share no step
    # explains is listed on its outcome, ownership follows the receipt, and no record stays.
    recovered = setting.recover(record)
    (issue,) = recovered["outcomes"][-1]["accounting_issues"]
    assert (recovered["resolved"], operations(recovered)) == (True, ["decrease"])
    assert (str(setting.pool.shares_asset) in issue, shares(recovered)) == (True, held - burned)
    assert_settled(record)
    # The wallet owns what it holds: the next command adopts the whole balance.
    closed = close_position(setting, held - burned + 1, "balanced")
    assert sum(closed.values()) > 0


def test_every_declared_cell_has_a_reference_and_every_variant_is_rehearsed():
    cells = declared_cells()
    found = [reference(chain, variant) for chain, variant in cells]
    kinds = {"open_liquidity", "increase_liquidity", "decrease_liquidity", "close_liquidity"}
    declared = connector_from_wire({"connector": "curve"}).chain_support
    assert all(kinds <= {str(a) for a in s.direct_actions} for s in declared if s.direct_actions)
    assert {variant for _, variant in cells} == set(FAMILIES)
    assert all(item.funded // 4 > 0 for item in found)
