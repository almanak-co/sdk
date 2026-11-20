"""Morpho Blue direct operator actions: the declaration, without signing or chain access.

Component evidence from literal market data and retained strategy checkpoints. The
whole command is rehearsed on forks in tests/fork/test_ax_morpho_blue.py.
"""

import json
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.ax import lending_request
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct as declarations
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.connectors.morpho_blue import capacity, direct
from almanak.connectors.morpho_blue.authority import admit, declared
from almanak.connectors.morpho_blue.direct import OTHER_LEG
from almanak.connectors.morpho_blue.domain import AccountObservation, Position, Reserve
from almanak.connectors.morpho_blue.lifecycle import MorphoLifecycle, validate_owned
from almanak.connectors.morpho_blue.observations import Ownership, Snapshot
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.planning import operation
from almanak.connectors.morpho_blue.reporting import report
from almanak.connectors.morpho_blue.risk import borrowed_assets
from almanak.connectors.morpho_blue.state import Checkpoint
from almanak.connectors.morpho_blue.support import DIRECT_CHAINS, candidate_support
from almanak.connectors.pool_lending_direct import REPAY_FLOOR
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from almanak.operator_deployment import adopts, supports_operator
from almanak.state import Journal
from tests.lending_captures import read_capture
from tests.morpho_blue_helpers import CASES, EVIDENCE
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho
from tests.test_morpho_blue_offline import CHAIN, MARKET, SHARES, USDC, WETH, observation

WALLET = Wallet(account="treasury", address="0x" + "ab" * 20)
BLOCK = observation().block
# 1 WETH, worth 2000 USDC at the observed price, posted against 100 USDC of debt, beside
# 500 USDC lent. The market's LLTV of 0.86 puts the health factor at 17.2.
INDEBTED = Position(
    supply_shares=500 * 10**6 * SHARES, borrow_shares=100 * 10**6 * SHARES, collateral=10**18
)
EMPTY = Position(supply_shares=0, borrow_shares=0, collateral=0)
POSTED = Position(supply_shares=0, borrow_shares=0, collateral=10**18)
FLOOR = 15 * 10**17
SUPPLY = ActionRequest("supply", 1)


class Positions:
    """A reader that answers the core's `position` call and records what was asked."""

    def __init__(self, held: Position):
        self.held, self.calls = held, []

    def call(self, to, signature, block, *, inputs, args, outputs):
        self.calls.append((to, signature, block, args))
        return (self.held.supply_shares, self.held.borrow_shares, self.held.collateral)


def bound(asset, held=EMPTY, request=SUPPLY):
    reader = Positions(held)
    resolved = SimpleNamespace(reserve=Reserve(market=MARKET, asset=asset))
    return reader, direct.positions(reader, BLOCK, resolved, WALLET, request)


def configured(asset, held=EMPTY, request=SUPPLY):
    _, document = bound(asset, held, request)
    return ax_actions.configuration(
        "morpho_blue",
        document,
        WALLET,
        ax_actions.alchemy(CHAIN.chain_id),
        CHAIN,
        ax_actions.FeeLimits(max_gas_price_wei=5, max_transaction_fee=9),
    )


def observed(config, held, *, allowance=0):
    """Each configured leg as the gateway observes it, at one block."""
    snapshots = []
    for allocation in config.allocations:
        target = config.target(allocation.name)
        account = AccountObservation(market=observation(), fund=target.fund, position=held)
        snapshots.append(
            Snapshot(
                target=target,
                account=account,
                wallet_balance=10**24,
                allowance=allowance if allocation.name == ALLOCATION else 0,
                native_balance=AssetAmount(asset=native_asset(CHAIN.chain_id), amount=10**18),
                payer=target.fund,
            )
        )
    return tuple(snapshots)


def retained(config, intent):
    return SimpleNamespace(
        capabilities=declared(config),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=(),
    )


def planned(asset, held, request):
    """The connector's own economic step for a request, after its capacity checks."""
    config = configured(asset, held, request)
    snapshots = observed(config, held)
    intent = direct.intent(config, snapshots, request)
    before = direct.requested(snapshots)
    step = operation(config, retained(config, intent), intent, before)
    if isinstance(step, ApproveOperation):
        before = before.model_copy(update={"allowance": step.amount.amount})
        step = operation(config, retained(config, intent), intent, before)
    return step, capacity.validate(config, before, step)


def test_morpho_blue_declares_every_lending_action_where_it_is_rehearsed():
    assert supports_operator(Checkpoint)
    assert "morpho_blue" not in declarations.NOT_DIRECT
    assert declarations.binding("morpho_blue").actions == ("supply", "withdraw", "borrow", "repay")
    cells = {str(cell.chain): cell for cell in candidate_support()}
    # Every production chain but HyperEVM, whose provider does not serve reads at the head.
    assert set(cells) - DIRECT_CHAINS == {"eip155:999"}
    requests = (
        SUPPLY,
        ActionRequest("withdraw"),
        ActionRequest("withdraw", 5),
        ActionRequest("borrow", 5, FLOOR),
        ActionRequest("repay"),
        ActionRequest("repay", 5),
    )
    for chain, cell in cells.items():
        if chain not in DIRECT_CHAINS:
            assert cell.direct_actions == ()
            with pytest.raises(ValueError, match="serves no direct supply on eip155:999"):
                declarations.require_direct("morpho_blue", cell.chain, SUPPLY)
            continue
        # Every action a strategy may take on the cell is also a direct action.
        assert cell.direct_actions == cell.actions
        assert {str(kind) for kind in cell.actions} == {request.kind for request in requests}
        for request in requests:
            declarations.require_direct("morpho_blue", cell.chain, request)
        with pytest.raises(ValueError, match="morpho_blue serves no direct collateral"):
            declarations.require_direct(
                "morpho_blue", cell.chain, ActionRequest("collateral", enable=True)
            )


def parse(command, *flags, leg="loan"):
    arguments = [
        "--connector",
        "morpho_blue",
        "--chain",
        "eip155:1",
        "--market-id",
        str(MARKET.market_id),
        "--leg",
        leg,
        "--wallet",
        "wallet",
        "--env-file",
        ".env",
        "--max-gas-price-wei",
        "5",
        "--max-transaction-fee",
        "9",
    ]
    return parse_validated(build_parser(), ["ax", command, *arguments, *flags])


def test_the_commands_take_a_market_a_leg_and_their_own_flags(capsys):
    supply = parse("supply", "--amount", "7", leg="collateral")
    assert (supply.product, supply.connector, supply.amount) == (
        "operator-action",
        "morpho_blue",
        7,
    )
    selected = lending_request(supply)
    assert (selected.market_id, selected.leg) == (MARKET.market_id, "collateral")
    withdraw = parse("withdraw")
    assert withdraw.amount is None and not withdraw.execute
    partial = parse("withdraw", "--amount", "3", "--min-health-factor", "1.5", leg="collateral")
    assert (partial.amount, partial.min_health_factor) == (3, FLOOR)
    borrow = parse("borrow", "--amount", "7", "--min-health-factor", "1.5")
    assert (borrow.amount, borrow.min_health_factor) == (7, FLOOR)
    assert (parse("repay").amount, parse("repay", "--amount", "7").amount) == (None, 7)
    for refused in (
        ("borrow", "--amount", "7"),
        ("repay", "--min-health-factor", "1.5"),
        ("collateral", "--enable"),
    ):
        with pytest.raises(SystemExit):
            parse(*refused)
    capsys.readouterr()


@pytest.mark.parametrize("case", CASES, ids=lambda row: row["name"][-12:])
def test_a_strategy_checkpoint_keeps_its_wire_form(case):
    text = read_capture(EVIDENCE / case["name"] / "checkpoint.json.gz")
    state = Checkpoint.model_validate_json(text)
    wire = state.model_dump_json()
    # Field for field what the capture holds: no operator marker, and the strategy kept.
    assert json.loads(wire) == json.loads(text)
    assert not state.operator and "operator" not in json.loads(wire)
    assert Checkpoint.model_validate_json(wire).model_dump_json() == wire
    orphan = {**json.loads(wire), "strategy": None}
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps(orphan))


def test_an_operator_checkpoint_round_trips_without_a_strategy(tmp_path, recorded_morpho):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded_morpho.config, None)
    state = journal.load()
    assert state.operator and state.strategy is None and not state.initialized
    assert state.capabilities == declared(recorded_morpho.config)
    assert all(item.empty for item in state.owned)
    wire = json.loads(state.model_dump_json())
    assert wire["operator"] is True and wire["strategy"] is None
    assert Checkpoint.model_validate_json(json.dumps(wire)) == state
    assert report(state)["strategy"] is None
    assert report(recorded_morpho)["strategy"]["sha256"] == recorded_morpho.strategy.sha256
    claimed = {**wire, "strategy": recorded_morpho.strategy.model_dump(mode="json")}
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        Checkpoint.model_validate_json(json.dumps(claimed))


def test_adoption_takes_ownership_from_the_observed_position(recorded_morpho):
    empty = MorphoLifecycle.initial(
        recorded_morpho.config, None, recorded_morpho.capabilities, None
    )
    assert MorphoLifecycle.adopt(empty.owned, recorded_morpho.baseline) == empty.owned
    for outcome in recorded_morpho.outcomes:
        owned = tuple(item for item in empty.owned if item.target == outcome.operation.target)
        adopted = MorphoLifecycle.adopt(owned, (outcome.after,))
        assert adopted == MorphoLifecycle.commit_owned(owned, outcome)
    # The observations of one decision taken while debt was open: every leg at one block.
    repay = next(item for item in recorded_morpho.outcomes if item.operation.kind == "repay")
    seen = repay.decision.observations
    adopted = MorphoLifecycle.adopt(empty.owned, seen)
    assert any(item.borrow_shares for item in adopted)
    assert any(item.collateral for item in adopted)
    holding = empty.model_copy(update={"initialized": True, "baseline": seen, "owned": adopted})
    assert adopts(empty, holding)
    validate_owned(holding, seen)
    with pytest.raises(DataError, match="must cover every configured Morpho allocation"):
        MorphoLifecycle.adopt(empty.owned, seen[:1])


@pytest.mark.parametrize(
    ("asset", "held", "legs"),
    [
        (USDC, EMPTY, [USDC]),
        (WETH, EMPTY, [WETH]),
        # The requested leg alone needs no second allocation.
        (USDC, Position(supply_shares=7, borrow_shares=3, collateral=0), [USDC]),
        (WETH, POSTED, [WETH]),
        (USDC, POSTED, [USDC, WETH]),
        (WETH, Position(supply_shares=7, borrow_shares=0, collateral=0), [WETH, USDC]),
        (WETH, Position(supply_shares=0, borrow_shares=3, collateral=7), [WETH, USDC]),
        (USDC, INDEBTED, [USDC, WETH]),
    ],
)
def test_the_deployment_binds_every_leg_the_wallet_holds(asset, held, legs):
    reader, document = bound(asset, held)
    assert reader.calls == [
        (
            MARKET.core,
            "position(bytes32,address)",
            BLOCK,
            (bytes.fromhex(str(MARKET.market_id)[2:]), WALLET.address),
        )
    ]
    assert document["connector"] == "morpho_blue"
    names = [ALLOCATION, OTHER_LEG][: len(legs)]
    assert [item["name"] for item in document["allocations"]] == names
    assert [item["reserve"]["asset"] for item in document["allocations"]] == [str(a) for a in legs]
    config = configured(asset, held)
    assert declarations.binding_for(config) is declarations.binding("morpho_blue")
    assert config.target(ALLOCATION).reserve == Reserve(market=MARKET, asset=asset)
    assert config.target(ALLOCATION).fund.address == WALLET.address
    # A supply or a withdrawal carries no borrow authority, and no floor unless stated.
    for allocation in config.allocations:
        assert (allocation.account, allocation.minimum_native_balance) == ("treasury", 0)
        assert not allocation.borrow and allocation.minimum_health_factor is None
    assert not any(item.borrow for item in declared(config).requirements)
    # The ownership check, unchanged, accepts exactly this binding of the position.
    snapshots = observed(config, held)
    state = SimpleNamespace(owned=adopt(config, snapshots))
    validate_owned(state, snapshots)


def adopt(config, snapshots):
    empty = tuple(
        Ownership(target=config.target(a.name), supply_shares=0, borrow_shares=0, collateral=0)
        for a in config.allocations
    )
    return MorphoLifecycle.adopt(empty, snapshots)


def test_a_single_leg_binding_of_a_two_leg_position_is_still_refused():
    config = configured(USDC)
    held = Position(supply_shares=7, borrow_shares=0, collateral=7)
    snapshots = observed(config, held)
    state = SimpleNamespace(owned=adopt(config, snapshots))
    with pytest.raises(DataError, match="not owned by this deployment"):
        validate_owned(state, snapshots)


@pytest.mark.parametrize(
    ("request_", "held", "floor"),
    [
        (ActionRequest("borrow", 5, FLOOR), POSTED, FLOOR),
        (ActionRequest("borrow", 5, FLOOR), INDEBTED, FLOOR),
        # A repayment states the lowest valid floor, and binds the collateral leg its
        # borrow authority needs even when nothing is posted.
        (ActionRequest("repay"), INDEBTED, REPAY_FLOOR),
        (ActionRequest("repay", 5), INDEBTED, REPAY_FLOOR),
        (ActionRequest("repay"), EMPTY, REPAY_FLOOR),
    ],
)
def test_a_borrow_or_a_repayment_binds_both_legs_with_borrow_authority(request_, held, floor):
    config = configured(USDC, held, request_)
    loan, collateral = config.allocations
    assert (loan.name, loan.reserve.asset, loan.borrow) == (ALLOCATION, USDC, True)
    assert (collateral.name, collateral.reserve.asset, collateral.borrow) == (
        OTHER_LEG,
        WETH,
        False,
    )
    # One floor for the fund's two legs in the market, as the connector requires.
    assert loan.minimum_health_factor == collateral.minimum_health_factor == floor
    capabilities = declared(config)
    assert [item.borrow for item in capabilities.requirements] == [True, False]
    for intent in (config.borrow(ALLOCATION, 1), config.repay(ALLOCATION)):
        admit(capabilities, intent)
    snapshots = observed(config, held)
    validate_owned(SimpleNamespace(owned=adopt(config, snapshots)), snapshots)


@pytest.mark.parametrize(
    ("asset", "held", "request_"),
    [
        (USDC, EMPTY, ActionRequest("supply", 5, FLOOR)),
        (WETH, POSTED, ActionRequest("withdraw", None, FLOOR)),
        (WETH, INDEBTED, ActionRequest("withdraw", 5, FLOOR)),
        (WETH, INDEBTED, ActionRequest("supply", 5, FLOOR)),
    ],
)
def test_a_stated_floor_serves_every_bound_leg_without_borrow_authority(asset, held, request_):
    config = configured(asset, held, request_)
    assert {item.minimum_health_factor for item in config.allocations} == {FLOOR}
    assert not any(item.borrow for item in config.allocations)
    for intent in (config.borrow(ALLOCATION, 1), config.repay(ALLOCATION)):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(declared(config), intent)


@pytest.mark.parametrize(
    ("asset", "held", "request_", "message"),
    [
        (WETH, POSTED, ActionRequest("borrow", 5, FLOOR), "a borrow is served on the loan leg"),
        (WETH, INDEBTED, ActionRequest("repay"), "a repay is served on the loan leg"),
        (USDC, EMPTY, ActionRequest("borrow", 5, FLOOR), "the wallet has no collateral"),
        (
            USDC,
            Position(supply_shares=7, borrow_shares=0, collateral=0),
            ActionRequest("borrow", 5, FLOOR),
            "supply the collateral leg before a borrow",
        ),
        (USDC, POSTED, ActionRequest("borrow", 5), "a borrow is risk-checked.*--min-health-factor"),
        # Collateral that backs debt never leaves in full, with or without a floor.
        (WETH, INDEBTED, ActionRequest("withdraw"), "cannot be withdrawn in full.*`ax repay`"),
        (
            WETH,
            INDEBTED,
            ActionRequest("withdraw", None, FLOOR),
            "withdraw an --amount under --min-health-factor",
        ),
        (
            WETH,
            INDEBTED,
            ActionRequest("withdraw", 5),
            "a collateral withdraw there is risk-checked against a health floor; "
            "state --min-health-factor, or repay the debt with `ax repay`",
        ),
    ],
)
def test_a_request_the_leg_cannot_serve_is_refused_before_anything_is_planned(
    asset, held, request_, message
):
    with pytest.raises(ValueError, match=message):
        bound(asset, held, request_)


@pytest.mark.parametrize(
    ("asset", "held", "request_"),
    [
        # None of these can lower the position's health, so none needs a floor.
        (USDC, INDEBTED, ActionRequest("withdraw")),
        (USDC, INDEBTED, ActionRequest("withdraw", 5)),
        (USDC, INDEBTED, ActionRequest("supply", 5)),
        (WETH, INDEBTED, ActionRequest("supply", 5)),
        (USDC, INDEBTED, ActionRequest("repay")),
        # Collateral that backs nothing leaves without one.
        (WETH, POSTED, ActionRequest("withdraw")),
        (WETH, POSTED, ActionRequest("withdraw", 5)),
    ],
)
def test_no_floor_is_asked_where_nothing_is_risk_checked(asset, held, request_):
    _, document = bound(asset, held, request_)
    floors = {item.get("minimum_health_factor") for item in document["allocations"]}
    assert floors == ({REPAY_FLOOR} if request_.action == "repay" else {None})


def test_each_command_becomes_its_intent():
    config = configured(USDC, INDEBTED, ActionRequest("repay"))
    snapshots = observed(config, INDEBTED)
    target = config.target(ALLOCATION)

    def intent(action, amount=None):
        return direct.intent(config, snapshots, ActionRequest(action, amount))

    for action, amount, kind in (
        ("supply", 7, "supply"),
        ("borrow", 7, "borrow"),
        ("withdraw", None, "withdraw"),
        ("withdraw", 7, "withdraw_amount"),
        ("repay", None, "repay"),
        ("repay", 7, "repay_amount"),
    ):
        built = intent(action, amount)
        assert (built.kind, built.target) == (kind, target)
        assert ActionRequest(action, amount).kind == kind
        assert amount is None or built.amount.amount == amount
    for action in ("supply", "borrow"):
        with pytest.raises(ValueError, match=f"{action} requires --amount"):
            intent(action)
    with pytest.raises(ValueError, match="requested position was not observed"):
        direct.intent(config, (), ActionRequest("withdraw"))


def test_a_withdrawal_needs_a_supplied_balance_or_an_allowance_to_revoke():
    config = configured(USDC)
    with pytest.raises(DataError, match="nothing is supplied on this leg") as refusal:
        direct.intent(config, observed(config, EMPTY), ActionRequest("withdraw"))
    assert refusal.value.kind == "no_position" and "owes" not in str(refusal.value)
    # Debt alone is not a supplied balance, and the refusal names the command for it.
    owing = Position(supply_shares=0, borrow_shares=3, collateral=7)
    config = configured(USDC, owing)
    with pytest.raises(DataError, match="owes 3 borrow shares, which `ax repay` repays"):
        direct.intent(config, observed(config, owing), ActionRequest("withdraw", 5))
    stray = observed(config, owing, allowance=7)
    assert direct.intent(config, stray, ActionRequest("withdraw")).kind == "withdraw"


def test_a_partial_withdrawal_stays_within_the_supplied_balance():
    lending = configured(USDC, INDEBTED)
    snapshots = observed(lending, INDEBTED)
    # 500 USDC lent in a market with no interest accrued at the observed block.
    assert direct.intent(lending, snapshots, ActionRequest("withdraw", 500 * 10**6))
    with pytest.raises(DataError, match="exceeds the supplied balance of 500000000") as refusal:
        direct.intent(lending, snapshots, ActionRequest("withdraw", 500 * 10**6 + 1))
    assert refusal.value.kind == "exceeds_supplied"
    posting = configured(WETH, POSTED)
    snapshots = observed(posting, POSTED)
    assert direct.intent(posting, snapshots, ActionRequest("withdraw", 10**18))
    with pytest.raises(DataError, match=f"exceeds the supplied balance of {10**18} base units"):
        direct.intent(posting, snapshots, ActionRequest("withdraw", 10**18 + 1))


def test_a_supply_or_a_partial_repayment_uses_no_allowance_or_exactly_its_own():
    config = configured(USDC, INDEBTED, ActionRequest("repay", 7))
    approved = observed(config, INDEBTED, allowance=7)
    for action, exit_ in (("supply", "withdraw"), ("repay", "repay without --amount")):
        assert direct.intent(config, approved, ActionRequest(action, 7)).amount.amount == 7
        with pytest.raises(DataError, match=f"{action} allowance differs.*or {exit_}") as refusal:
            direct.intent(config, approved, ActionRequest(action, 8))
        assert refusal.value.kind == "allowance_mismatch"
        assert "the wallet approves 7 base units to Morpho" in str(refusal.value)
    # A withdrawal and a full repayment clear a stray allowance, so neither is refused.
    assert direct.intent(config, approved, ActionRequest("withdraw")).kind == "withdraw"
    assert direct.intent(config, approved, ActionRequest("repay")).kind == "repay"


def test_a_repayment_needs_debt_or_an_allowance_to_revoke():
    config = configured(USDC, EMPTY, ActionRequest("repay"))
    for amount in (None, 7):
        with pytest.raises(DataError, match="owes no debt in this market") as refusal:
            direct.intent(config, observed(config, EMPTY), ActionRequest("repay", amount))
        assert refusal.value.kind == "no_debt"
    # An allowance an interrupted repayment left is revoked by restating it.
    stray = observed(config, EMPTY, allowance=7)
    assert direct.intent(config, stray, ActionRequest("repay")).kind == "repay"


def test_the_result_names_the_requested_leg_and_the_other_when_bound():
    config = configured(WETH, INDEBTED)
    owned = adopt(config, observed(config, INDEBTED))
    # Found by name: the order of the retained ownership does not matter. The debt is
    # the loan leg's borrow shares, bound here because the wallet holds them.
    for order in (owned, owned[::-1]):
        assert direct.position(SimpleNamespace(owned=order)) == {
            "leg": "collateral",
            "collateral": str(10**18),
            "other_leg": {
                "leg": "loan",
                "supply_shares": str(500 * 10**6 * SHARES),
                "borrow_shares": str(100 * 10**6 * SHARES),
            },
        }
    single = configured(USDC)
    assert direct.position(SimpleNamespace(owned=adopt(single, observed(single, EMPTY)))) == {
        "leg": "loan",
        "supply_shares": "0",
        "borrow_shares": "0",
    }


def test_a_borrow_is_admitted_above_its_floor_and_refused_below_it():
    step, result = planned(USDC, POSTED, ActionRequest("borrow", 100 * 10**6, FLOOR))
    assert isinstance(step, BorrowOperation) and step.amount.amount == 100 * 10**6
    assert result.position.borrow_shares > 0 and result.position.collateral == 10**18
    # 1 WETH backs 1720 USDC of debt at the LLTV, so 1146 USDC at a floor of 1.5.
    planned(USDC, POSTED, ActionRequest("borrow", 1146 * 10**6, FLOOR))
    with pytest.raises(ValueError, match="violates the configured fund/market health floor"):
        planned(USDC, POSTED, ActionRequest("borrow", 1147 * 10**6, FLOOR))
    with pytest.raises(ValueError, match="violates the configured fund/market health floor"):
        planned(USDC, INDEBTED, ActionRequest("borrow", 10**6, 18 * 10**18))


def test_collateral_that_backs_debt_leaves_in_part_above_a_stated_floor():
    step, result = planned(WETH, INDEBTED, ActionRequest("withdraw", 10**17, FLOOR))
    assert isinstance(step, WithdrawOperation) and not step.full
    assert result.position.collateral == 9 * 10**17
    assert result.position.borrow_shares == INDEBTED.borrow_shares
    # 100 USDC of debt at a floor of 1.5 needs 150 / 1720 of a WETH left behind.
    with pytest.raises(ValueError, match="violates the configured fund/market health floor"):
        planned(WETH, INDEBTED, ActionRequest("withdraw", 92 * 10**16, FLOOR))
    # The same wallet without debt withdraws all of it, with no floor.
    step, result = planned(WETH, POSTED, ActionRequest("withdraw"))
    assert isinstance(step, WithdrawOperation) and result.position.collateral == 0


def test_a_repayment_is_never_refused_by_a_floor():
    debt = borrowed_assets(observed(configured(USDC, INDEBTED), INDEBTED)[0].account)
    step, result = planned(USDC, INDEBTED, ActionRequest("repay", debt // 2))
    assert isinstance(step, RepayOperation) and not step.full
    assert 0 < result.position.borrow_shares < INDEBTED.borrow_shares
    step, result = planned(USDC, INDEBTED, ActionRequest("repay"))
    assert isinstance(step, RepayOperation) and step.shares == INDEBTED.borrow_shares
    assert (result.position.borrow_shares, result.position.collateral) == (0, 10**18)
    # A position already below any floor an operator could state is still repaid: the
    # collateral is worth a thousandth of the debt here.
    under = INDEBTED.model_copy(update={"collateral": 10**12})
    step, result = planned(USDC, under, ActionRequest("repay"))
    assert isinstance(step, RepayOperation) and result.position.borrow_shares == 0
    with pytest.raises(DataError, match="partial repayment exceeds the owned debt"):
        planned(USDC, INDEBTED, ActionRequest("repay", debt + 1))


def test_a_wallet_with_debt_keeps_every_action_that_cannot_lower_its_health():
    step, result = planned(WETH, INDEBTED, ActionRequest("supply", 10**17))
    assert isinstance(step, SupplyOperation)
    assert result.position.collateral == INDEBTED.collateral + 10**17
    step, result = planned(USDC, INDEBTED, ActionRequest("supply", 10**6))
    assert isinstance(step, SupplyOperation)
    assert result.position.supply_shares > INDEBTED.supply_shares
    # No stated floor refuses a withdrawal of the lent balance, in full or in part.
    for floor in (None, 10**60):
        step, result = planned(USDC, INDEBTED, ActionRequest("withdraw", None, floor))
        assert isinstance(step, WithdrawOperation) and step.shares == INDEBTED.supply_shares
        assert (result.position.supply_shares, result.position.borrow_shares) == (
            0,
            INDEBTED.borrow_shares,
        )
        step, result = planned(USDC, INDEBTED, ActionRequest("withdraw", 10**6, floor))
        assert 0 < result.position.supply_shares < INDEBTED.supply_shares
    # A deployment built for a supply or a withdrawal can neither borrow nor repay.
    lending = configured(USDC, INDEBTED)
    for intent in (lending.borrow(ALLOCATION, 1), lending.repay(ALLOCATION)):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(declared(lending), intent)
