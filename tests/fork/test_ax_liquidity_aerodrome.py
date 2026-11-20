"""The direct liquidity commands of `almanak ax` on deployed Aerodrome stable and volatile pools.

Both invariants of the one cell that declares the liquidity actions as direct actions
are rehearsed, each step as its own command with its own deployment: an isolated Anvil
fork of Base stands in for mainnet behind the production adapter's own planning, signing
and reconciliation, and the command's own resolution builds each deployment. The fork
keeps the time of its pinned block. It is rehearsal evidence, not a mainnet run.
"""

from pathlib import Path

import pytest
import yaml

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.aerodrome import math
from almanak.connectors.aerodrome.position_request import (
    PositionRequest,
    read_liquidity_position,
)
from almanak.connectors.composition import connector_from_wire
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import balance, read_uint
from almanak.gateway.read_endpoints import read_endpoint
from almanak.gateway.safe_fixture import calldata
from almanak.models import Chain
from tests.fork.test_ax_liquidity import (
    FAR_DEADLINE,
    LIMITS,
    TRADE_LIMITS,
    TRADER_MULTIPLE,
    WALLET_MULTIPLE,
    Reference,
    amounts,
    approved,
    assert_blocked,
    assert_clean,
    assert_mint_reverted,
    assert_moved,
    assert_released,
    assert_unused,
    funded_wallet,
    halves,
    operations,
    recover_interrupted_approval,
    refused_after_approvals,
    reported_allowances,
    revoke_everything,
    shortfalls,
    stopped_while_waiting_for,
    troubled,
)
from tests.fork.test_ax_liquidity import Setting as RangedSetting
from tests.fork.test_ax_swap_aerodrome import as_mainnet, share_sent_before, share_sent_with

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
CONNECTOR = "aerodrome"
BASE = ChainId("eip155:8453")
ADD_LIQUIDITY = (
    "address",
    "address",
    "bool",
    "uint256",
    "uint256",
    "uint256",
    "uint256",
    "address",
    "uint256",
)


def declared_variants():
    """The pool invariants of the cell that declares opening liquidity as a direct action."""
    cells = connector_from_wire({"connector": CONNECTOR}).chain_support
    return [
        str(variant)
        for support in cells
        if "open_liquidity" in {str(action) for action in support.direct_actions}
        for variant in support.variants
    ]


def reference(variant: str) -> Reference:
    """The pinned block, pool and funded asset of the invariant's liquidity reference."""
    name = f"aerodrome_base_{variant}_lp"
    fork = yaml.safe_load((ROOT / "examples/operators" / f"{name}.fork.yaml").read_text())["fork"]
    config = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    (anchor,) = fork["anchors"]
    (fund,) = fork["funds"]
    (funded,) = fund["assets"]
    (operator,) = fork["operators"]
    return Reference(
        connector=CONNECTOR,
        chain=BASE,
        pool=config["markets"]["pool"]["pool"],
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


class Setting(RangedSetting):
    """One funded wallet on one fork of Base; each call is one command with its own
    deployment. A position is the wallet's balance of the pool's own share token."""

    def __init__(self, fork, cell, wallet, signer, identity):
        self.fork, self.cell, self.wallet, self.identity = fork, cell, wallet, identity
        self.signer = signer
        self.owner = wallet["address"].lower()
        resolution = ax_actions.resolution(cell.connector)
        self.market = resolution.MarketSelection(
            connector=cell.connector, chain=cell.chain, pool=cell.pool
        )
        with self.reader() as reader:
            resolved = resolution.resolve_market(reader, self.market, reader.pin_block("latest"))
        self.pool, self.contracts = resolved.pool, resolved.contracts
        self.assets = (self.pool.currency0, self.pool.currency1)
        self.tokens = tuple(erc20_address(asset, cell.chain) for asset in self.assets)
        # The one spender of every step. The helpers shared with the V3 family's
        # rehearsal call the spender a deposit approves the manager.
        self.manager = self.contracts.router
        self.funded = self.assets.index(cell.asset)

    def other(self, wallet, signer):
        """The same pool for another wallet on the same fork."""
        return Setting(self.fork, self.cell, wallet, signer, self.identity)

    def offered(self, stated):
        """What a deposit of the stated amounts offers at the pool's reserves: what it
        takes of each currency in their proportion."""
        pool = self.pool_state()
        most = stated["amount0"], stated["amount1"]
        return math.proportional(pool.reserve0, pool.reserve1, pool.total_supply, *most)

    def proportion(self):
        """The pool's reserves of currency0 and currency1."""
        pool = self.pool_state()
        return pool.reserve0, pool.reserve1

    def shares(self):
        """The wallet's balance of the pool's share token, in base units."""
        return balance(self.fork.rpc, self.pool.pool, self.owner)

    def allowances(self):
        """Every nonzero allowance the wallet gave the router: currencies, then shares."""
        spent = zip(
            (*self.assets, self.pool.shares_asset), (*self.tokens, self.pool.pool), strict=True
        )
        granted = {
            (str(asset), self.manager): read_uint(
                self.fork.rpc, token, "allowance(address,address)", (self.owner, self.manager)
            )
            for asset, token in spent
        }
        return {key: amount for key, amount in granted.items() if amount}

    def read(self):
        """`ax liquidity-position` at the fork's head, through the read's own function."""
        request = PositionRequest(chain=self.cell.chain, pool=self.cell.pool, account=self.owner)
        with self.reader() as reader:
            return read_liquidity_position(reader, request, reader.pin_block("latest"))

    def owed(self):
        """What a claim would pay the wallet, as the read reports it."""
        return [int(item["amount"]) for item in self.read()["owed"]]

    def add_outside(self, amount0, amount1):
        """Deposit straight through the router, outside `ax`: approve both, then add."""
        for token, amount in zip(self.tokens, (amount0, amount1), strict=True):
            approval = calldata(
                "approve(address,uint256)", ("address", "uint256"), (self.manager, amount)
            )
            self.send(token, approval)
        stated = (*self.tokens, self.pool.stable, amount0, amount1, 0, 0, self.owner, FAR_DEADLINE)
        signature = "addLiquidity(" + ",".join(ADD_LIQUIDITY) + ")"
        self.send(self.manager, calldata(signature, ADD_LIQUIDITY, stated))

    def leave_in_pool(self, shares):
        """Send shares to the pool's own balance, as a stray transfer would."""
        transfer = calldata(
            "transfer(address,uint256)", ("address", "uint256"), (self.pool.pool, shares)
        )
        self.send(self.pool.pool, transfer)


@pytest.fixture
def setting(fork, cell, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet, signer = funded_wallet(fork, cell, tmp_path / "wallet", cell.funded * WALLET_MULTIPLE)
    # Two trades, each as large as the wallet's funding, make fees accrue twice.
    trading = cell.funded * (2 * TRADER_MULTIPLE + 1)
    trader = funded_wallet(fork, cell, tmp_path / "trader", trading)
    identity = as_mainnet(fork, cell.chain)
    found = Setting(fork, cell, wallet, signer, identity)
    found.trader = found.other(*trader)
    return found


def held_shares(result, moment="position_after"):
    """The shares a result reports the deployment owns."""
    return int(result[moment]["position"]["shares"]["amount"])


def assert_previewed(setting, stated):
    """A preview plans the first approval, attributes the stated amounts and signs nothing."""
    start = setting.balances()
    preview = setting("open-liquidity", execute=False, **stated)
    step = preview["first_step"]["operation"]["kind"]
    assert (preview["action"], preview["executed"], step) == ("open-liquidity", False, "approve")
    assert preview["position_before"]["position"] is None
    assert amounts(preview, "position_before", "inventory") == list(stated.values())
    assert (setting.balances(), setting.allowances(), setting.shares()) == (start, {}, 0)


def open_position(setting, stated):
    """Make the first deposit; returns the shares it minted and what it used."""
    start, offer = setting.balances(), setting.offered(stated)
    opened = setting("open-liquidity", **stated)
    assert_clean(opened, ["approve", "approve", "mint"])
    used = [-moved for moved in assert_moved(setting, opened, start)]
    assert held_shares(opened) == setting.shares() > 0
    # Mined at the proportion it was offered at, the deposit takes its whole offer:
    # the rest of the statement is unused and nothing stays approved.
    assert_unused(setting, opened, stated, offer, used)
    assert (tuple(used), setting.allowances()) == (offer, {})
    return held_shares(opened), used


def rounding(seen):
    """The most a deposit puts in of each currency beyond the principal its shares
    return, in base units: what two shares are worth, for the shares a mint and a burn
    round away, and what one base unit of the other currency is worth, since the offer
    of one currency is a whole number of the other's proportion."""
    supply = int(seen["pool"]["total_supply"])
    held = [int(item["amount"]) for item in seen["pool"]["balances"]]
    return [2 * (own // supply + 1) + own // other + 1 for own, other in (held, held[::-1])]


def assert_read(setting, shares, used):
    """The read reports the wallet's shares and, as principal, what the deposit put in."""
    seen = setting.read()
    assert (seen["owner"], int(seen["position"]["shares"]["amount"])) == (setting.owner, shares)
    assert seen["position"]["liquidity"] == str(shares)
    # The principal is the deposit less what whole shares and whole base units round away.
    short = shortfalls(used, seen["principal"])
    assert all(0 <= lost <= most for lost, most in zip(short, rounding(seen), strict=True))
    assert [int(item["amount"]) for item in seen["owed"]] == [0, 0]


def increase_position(setting, shares, stated):
    """Add the stated amounts to the wallet's shares; returns the grown balance."""
    start = setting.balances()
    increased = setting("increase-liquidity", **stated)
    assert_clean(increased)
    kinds = operations(increased)
    assert (kinds[-1], set(kinds) <= {"approve", "increase"}) == ("increase", True)
    assert held_shares(increased, "position_before") == shares
    assert_moved(setting, increased, start)
    assert held_shares(increased) == setting.shares() > shares
    return held_shares(increased)


def earn_fees(setting):
    """Another account trades through the pool; returns what the wallet can then claim."""
    setting.trader.swap(setting.cell.funded * TRADER_MULTIPLE, TRADE_LIMITS)
    owed = setting.owed()
    assert owed[setting.funded] > 0
    return owed


def collect_fees(setting, owed, shares):
    """The claim pays exactly what the read reported, and every share is kept."""
    start = setting.balances()
    collected = setting("collect-liquidity-fees")
    assert_clean(collected, ["collect"])
    assert held_shares(collected, "position_before") == shares
    assert assert_moved(setting, collected, start) == owed
    assert (held_shares(collected), setting.shares()) == (shares, shares)
    assert amounts(collected, "position_after", "inventory") == owed
    return collected


def decrease_position(setting, shares):
    """Burn a third of the shares; all of what is left cannot be decreased away."""
    start, part = setting.balances(), shares // 3
    decreased = setting("decrease-liquidity", liquidity=part)
    assert_clean(decreased, ["approve", "decrease"])
    assert_released(setting, decreased, start)
    assert (held_shares(decreased), setting.shares()) == (shares - part, shares - part)
    with pytest.raises(ValueError, match="--liquidity must be below the"):
        setting("decrease-liquidity", liquidity=shares - part)


def close_position(setting, expected):
    """Burn every share. The wallet then holds none, and a second close finds nothing."""
    start = setting.balances()
    closed = setting("close-liquidity")
    assert_clean(closed, expected)
    assert_released(setting, closed, start)
    assert (closed["position_after"]["position"], setting.shares()) == (None, 0)
    with pytest.raises(ValueError, match="holds no shares of this pool, so close-liquidity"):
        setting("close-liquidity")


def test_shares_are_opened_read_increased_collected_decreased_closed_and_revoked(setting):
    first, more = halves(setting)
    assert_previewed(setting, first)
    shares, used = open_position(setting, first)
    assert_read(setting, shares, used)
    grown = increase_position(setting, shares, more)
    collect_fees(setting, earn_fees(setting), grown)
    decrease_position(setting, grown)
    # A trade since the claim: the burn pays principal, then the close claims the fees.
    earn_fees(setting)
    close_position(setting, ["approve", "close", "collect"])
    # Each deposit took its whole offer and each burn its whole approval.
    assert setting.allowances() == {}
    with pytest.raises(ValueError, match="no allowance in either pool currency"):
        setting("revoke-exchange")


def deposit_outside(setting):
    """Shares the wallet gets straight from the router, outside `ax`; returns their number."""
    setting.swap(setting.balances()[setting.funded] // 2)
    held = setting.balances()
    setting.add_outside(held[0] // 2, held[1] // 2)
    assert setting.shares() > 0
    return setting.shares()


def test_shares_obtained_outside_are_adopted_whole_then_collected_and_closed(setting):
    shares = deposit_outside(setting)
    assert int(setting.read()["position"]["shares"]["amount"]) == shares
    # The deployment owns every share the wallet holds, and the claim reconciles
    # against the checkpoints the pool stored for a deposit it never saw.
    collect_fees(setting, earn_fees(setting), shares)
    close_position(setting, ["approve", "close"])
    revoke_everything(setting)


def recover_interrupted_mint(setting, stated, start, monkeypatch, capsys):
    """Restated, the open reuses the approval that stands. Interrupted while its deposit
    waits, recovery reconciles the deposit from its receipt: the wallet holds shares."""
    record = stopped_while_waiting_for("mint", setting, monkeypatch, capsys, stated)
    assert_blocked(setting, record, "mint")
    recovered = setting.recover(record)
    ending = (recovered["resolved"], recovered["discarded_request"], troubled(recovered))
    assert (ending, operations(recovered)) == ((True, False, []), ["approve", "mint"])
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    assert held_shares(recovered) == setting.shares() > 0
    assert_moved(setting, recovered, start)


def test_an_open_that_is_refused_or_interrupted_reports_the_allowances_it_left(
    setting, monkeypatch, capsys
):
    first, _ = halves(setting)
    start = setting.balances()

    left = refused_after_approvals(setting, first, start)
    revoked = setting("revoke-exchange")
    assert_clean(revoked, ["approve"] * len(left))
    assert (setting.allowances(), setting.balances()) == ({}, start)

    recover_interrupted_approval(setting, first, start, monkeypatch, capsys)
    recover_interrupted_mint(setting, first, start, monkeypatch, capsys)


def close_refused_after_its_approval(setting, shares):
    """A gas limit that admits exactly the share approval refuses the burn that follows.

    The approval of every share to the router stands, as the result reports it, and no
    share moved.
    """
    gas = setting("close-liquidity", execute=False)["first_step"]["gas"]
    tight = ax_actions.FeeLimits(
        LIMITS.max_gas_price_wei, LIMITS.max_transaction_fee, max_gas_limit=gas
    )
    refused = setting("close-liquidity", limits=tight)
    ending = (refused["executed"], refused["completed"], refused["refused"]["kind"])
    assert (ending, operations(refused)) == ((True, False, "gas_limit"), ["approve"])
    granted = (str(setting.pool.shares_asset), setting.manager)
    assert (setting.allowances()[granted], reported_allowances(refused)) == (
        shares,
        setting.allowances(),
    )
    assert (held_shares(refused), setting.shares()) == (shares, shares)


def test_a_close_refused_after_its_share_approval_leaves_it_for_the_revocation(setting):
    shares = deposit_outside(setting)
    close_refused_after_its_approval(setting, shares)
    # The revocation clears the share allowance with the rest and burns nothing.
    revoke_everything(setting)
    assert setting.shares() == shares
    close_position(setting, ["approve", "close"])


def assert_share_adopted(setting, result, start, kind):
    """The step is reconciled from its own effects. The share nobody minted here is
    owned with the rest, and the step's outcome says the position was not all its own."""
    assert (result["completed"], operations(result)[-1]) == (True, kind)
    (odd,) = troubled(result)
    (issue,) = odd["accounting_issues"]
    assert (odd["operation"], odd["success"]) == (kind, True)
    assert str(setting.pool.shares_asset) in issue and "stays owned" in issue
    assert held_shares(result) == setting.shares()
    assert_moved(setting, result, start)


def test_a_share_sent_to_the_wallet_with_its_deposit_is_owned_and_reported(setting, monkeypatch):
    first, _ = halves(setting)
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_with("mint", setting, setting.pool.shares_asset, patched)
        opened = setting("open-liquidity", **first)
    assert_share_adopted(setting, opened, start, "mint")
    # The next command adopts the whole balance again and burns all of it cleanly.
    close_position(setting, ["approve", "close"])


def unbalanced(setting, bps):
    """A statement of all the wallet's funded asset and a twentieth of the other
    currency: about twenty times the funded asset the deposit takes."""
    held = setting.balances()
    stated = setting.stated(held[setting.funded], held[1 - setting.funded] // 20)
    return {**stated, "slippage_bps": bps}


def moved_by(before, after):
    """How far the proportion of the reserves moved, in basis points of what it was."""
    old, new = before[1] * after[0], after[1] * before[0]
    return abs(new - old) * 10000 // old


def proportion_move(setting, amount):
    """How far the trader's sale of `amount` moves the proportion of the pool's
    reserves, in basis points. The sale is measured on the fork and undone."""
    rpc, before = setting.fork.rpc, setting.proportion()
    snapshot = rpc("evm_snapshot")
    setting.trader.sell_outside(amount)()
    after = setting.proportion()
    assert rpc("evm_revert", [snapshot]) is True
    return moved_by(before, after)


def sized_sale(setting):
    """An amount of the funded asset whose sale by the trader moves the proportion by a
    few percent at most, and that move in basis points. A thin pool takes less."""
    amount = setting.trader.balances()[setting.funded]
    move = proportion_move(setting, amount)
    while move > 300:
        amount //= 4
        move = proportion_move(setting, amount)
    return amount, move


def assert_surplus_was_exposed(setting, stated, offer):
    """Offered whole, the same statement would be mined at the moved proportion: the
    router would take all of the other currency and more of the surplus than the
    bounded offer holds."""
    pool = setting.pool_state()
    whole = stated["amount0"], stated["amount1"]
    taken = math.addition(pool.reserve0, pool.reserve1, pool.total_supply, *whole)
    funded, other = setting.funded, 1 - setting.funded
    assert (taken[funded] > offer[funded], taken[other]) == (True, whole[other])


def test_a_deposit_mined_after_the_proportion_left_its_tolerance_reverts_and_takes_no_surplus(
    setting,
):
    halves(setting)
    selling, move = sized_sale(setting)
    # The tolerance is a third of what the sale moves the proportion by.
    tolerance = move // 3
    stated = unbalanced(setting, tolerance)
    offer, start, before = setting.offered(stated), setting.balances(), setting.proportion()
    # Another account sells the surplus currency once the deposit is signed: the
    # direction in which the router would take ever more of it.
    push = setting.trader.sell_outside(selling)
    result = setting("open-liquidity", before_send=("mint", push), **stated)
    assert (tolerance >= 1, moved_by(before, setting.proportion()) > tolerance) == (True, True)
    assert_mint_reverted(setting, result, offer, start)
    assert_surplus_was_exposed(setting, stated, offer)


def assert_took_within(setting, offer, used, tolerance):
    """The deposit took at most its offer of each currency and at least that less the
    tolerance. What it left of the offer stays approved, and nothing else is."""
    least = [most * (10000 - tolerance) // 10000 for most in offer]
    assert all(low <= spent <= most for low, spent, most in zip(least, used, offer, strict=True))
    left = [most - spent for most, spent in zip(offer, used, strict=True)]
    assert setting.allowances() == approved(setting, left)


def test_a_deposit_mined_after_a_move_inside_its_tolerance_takes_no_more_than_it_offered(setting):
    halves(setting)
    selling, move = sized_sale(setting)
    # The sale moves the proportion by a third of the tolerance.
    tolerance = max(move, 1) * 3
    stated = unbalanced(setting, tolerance)
    offer, start = setting.offered(stated), setting.balances()
    push = setting.trader.sell_outside(selling)
    result = setting("open-liquidity", before_send=("mint", push), **stated)
    assert_clean(result, ["approve", "approve", "mint"])
    used = [-moved for moved in assert_moved(setting, result, start)]
    assert_took_within(setting, offer, used, tolerance)


def test_a_share_sent_before_a_signed_claim_is_sent_does_not_hold_the_claim_back(
    setting, monkeypatch
):
    shares = deposit_outside(setting)
    earn_fees(setting)
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_before("collect", setting, setting.pool.shares_asset, patched)
        collected = setting("collect-liquidity-fees")
    # The claim carries no deadline: held back, it could never be sent or released.
    assert_share_adopted(setting, collected, start, "collect")
    assert (setting.shares(), ax_actions.kept_records()) == (shares + 1, [])


def test_a_share_sent_before_a_signed_share_approval_is_sent_does_not_hold_the_close_back(
    setting, monkeypatch
):
    deposit_outside(setting)
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_before("approve", setting, setting.pool.shares_asset, patched)
        closed = setting("close-liquidity")
    # The approval is sent and recorded with the arrival. The deployment owns the whole
    # balance, so the close then burns the share that arrived with the rest.
    (odd,) = troubled(closed)
    assert (closed["completed"], odd["operation"], operations(closed)[-1]) == (
        True,
        "approve",
        "close",
    )
    assert (closed["position_after"]["position"], setting.shares()) == (None, 0)
    assert_released(setting, closed, start)
    assert ax_actions.kept_records() == []


def test_a_share_sent_with_the_burn_leaves_the_close_complete_for_what_it_burned(
    setting, monkeypatch
):
    deposit_outside(setting)
    start = setting.balances()
    with monkeypatch.context() as patched:
        share_sent_with("close", setting, setting.pool.shares_asset, patched)
        closed = setting("close-liquidity")
    # Every share the close stated is burned. The one that arrived is not this
    # request's: it stays, reported once, with no second approval for it.
    assert_share_adopted(setting, closed, start, "close")
    assert (operations(closed), held_shares(closed)) == (["approve", "close"], 1)
    assert str(setting.pool.shares_asset) not in {asset for asset, _ in setting.allowances()}


def state_of(setting):
    return setting.balances(), setting.shares(), setting.allowances()


def refused(setting, error, match, action, **stated):
    """The command is refused before anything is signed: no record, and nothing moved."""
    before = state_of(setting)
    with pytest.raises(error, match=match):
        setting(action, **stated)
    assert (ax_actions.kept_records(), state_of(setting)) == ([], before)


def test_a_wallet_without_shares_can_only_make_a_first_deposit_it_covers(setting):
    first, _ = halves(setting)
    nothing = "has nothing to burn"
    refused(
        setting, ValueError, "open-liquidity makes the first deposit", "increase-liquidity", **first
    )
    refused(setting, ValueError, nothing, "decrease-liquidity", liquidity=5)
    refused(setting, ValueError, nothing, "close-liquidity")
    refused(setting, ValueError, "no fees to claim", "collect-liquidity-fees")
    refused(setting, ValueError, "no allowance in either pool currency", "revoke-exchange")
    # Half the wallet's balance was stated; five times that is more than it holds.
    beyond = {"amount0": first["amount0"] * 5, "amount1": first["amount1"]}
    refused(setting, DataError, "the wallet holds", "open-liquidity", **beyond)


def test_a_wallet_that_holds_shares_opens_no_second_position_and_trades_elsewhere(setting):
    first, more = halves(setting)
    shares, _ = open_position(setting, first)
    refused(setting, ValueError, "increase-liquidity adds to them", "open-liquidity", **more)
    refused(setting, ValueError, "must be below the", "decrease-liquidity", liquidity=shares)
    refused(setting, ValueError, "no fees to claim", "collect-liquidity-fees")
    # A swap is a trading-only deployment, which never owns the wallet's shares.
    selling = {"amount": more["amount0"], "sell": str(setting.assets[0])}
    refused(setting, DataError, "a trading-only allocation cannot own", "swap", **selling)
    # Shares left in the pool's own balance would be swept by a burn: no deposit and no
    # burn is planned while they are there.
    setting.leave_in_pool(1)
    stray = "pool already holds LP shares from another transfer"
    refused(setting, DataError, stray, "increase-liquidity", **more)
    refused(setting, DataError, stray, "close-liquidity")


def test_both_invariants_are_declared_and_have_a_liquidity_reference_to_rehearse():
    variants = declared_variants()
    found = [reference(variant) for variant in variants]
    assert variants == ["stable", "volatile"]
    assert len({item.pool for item in found}) == 2
    assert all(item.funded > 0 and item.asset.chain == item.chain for item in found)
