"""Direct borrow and full repayment on Aave V4: the deployment they build.

Component evidence against the retained two-Safe borrowing capture and a stub reader,
without signing or chain access. The whole commands on deployed contracts are
tests/fork/test_ax_aave_v4_debt.py.
"""

import json
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import lending_lookup
from almanak.connectors.aave_v4 import direct
from almanak.connectors.aave_v4 import execution as aave_execution
from almanak.connectors.aave_v4.authority import admit_intent, declared
from almanak.connectors.aave_v4.lifecycle import adopt_ownership, commit_ownership
from almanak.connectors.aave_v4.models import Ownership
from almanak.connectors.aave_v4.reader import plan_operation
from almanak.connectors.aave_v4.validation import validate_debt_operation, validate_owned
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet, binding, require_direct
from almanak.connectors.support import EconomicAction
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.models import Chain
from tests.test_aave_debt import borrowing_proof  # noqa: F401

WALLET = Wallet(account="treasury", address="0x" + "ab" * 20)
ORACLE = "0x" + "0c" * 20
ETHEREUM, AVALANCHE = ChainId("eip155:1"), ChainId("eip155:43114")
FLOOR = 15 * 10**17
# The capture's collateral and debt allocations of one fund, and their reserve IDs.
COLLATERAL, DEBT = "main_weth", "main_usdc"
WETH, USDC = 0, 7
BORROW = ActionRequest("borrow", 5, min_health_factor=FLOOR)
REPAY, WITHDRAW = ActionRequest("repay"), ActionRequest("withdraw")


def feed(reserve_id: int) -> str:
    return "0x" + f"{reserve_id + 1:02x}" * 20


class Spoke:
    """A Spoke with nine reserves, one wallet's position in each, and its oracle."""

    def __init__(self, factors=None, **held):
        # Reserve ID to what the wallet holds there; the rest is empty.
        self.held = {int(name.removeprefix("r")): values for name, values in held.items()}
        # Reserve ID to collateral factor in basis points; None forbids the read.
        self.factors = factors

    def call(self, target, signature, block, **options):
        if signature == "getReserveCount()":
            return (9,)
        if signature == "ORACLE()":
            return (ORACLE,)
        if signature == "getReserveSource(uint256)":
            assert target == ORACLE
            return (feed(options["args"][0]),)
        if signature in ("getReserve(uint256)", "getDynamicReserveConfig(uint256,uint32)"):
            return self.configuration(signature, *options["args"])
        reserve_id, account = options["args"]
        assert account == WALLET.address
        held = {"supplied": 0, "drawn": 0, "premium": 0, "offset": 0, "collateral": False}
        held.update(self.held.get(reserve_id, {}))
        if signature == "getUserPosition(uint256,address)":
            return (held["drawn"], held["premium"], held["offset"], held["supplied"], 0)
        assert signature == "getUserReserveStatus(uint256,address)"
        return (held["collateral"], bool(held["drawn"]))

    def configuration(self, signature, reserve_id, key=None):
        assert self.factors is not None, "a repayment reads no collateral factor"
        if key is None:
            # The reserve's current dynamic configuration key is its last field.
            return ("0x" + "00" * 20, "0x" + "00" * 20, 0, 18, 0, 0, 100 + reserve_id)
        assert key == 100 + reserve_id
        return (self.factors.get(reserve_id, 0), 0, 0)


def mainnet(reserve, reserve_id=None):
    """A captured fork reserve as the command resolves it, optionally as another ID."""
    document = json.loads(reserve.model_dump_json())
    document["chain"]["environment"] = "mainnet"
    if reserve_id is not None:
        document["reserve_id"] = reserve_id
    return type(reserve).model_validate_json(json.dumps(document))


@pytest.fixture
def market(borrowing_proof, monkeypatch):  # noqa: F811
    """Resolution of the capture's Spoke: its debt reserve, and any other reserve ID."""
    weth = borrowing_proof.config.allocation(COLLATERAL).reserve
    usdc = borrowing_proof.config.allocation(DEBT).reserve

    def resolved(reserve):
        chosen = SimpleNamespace(chain=reserve.chain.chain_id, spoke=reserve.spoke)
        return SimpleNamespace(reserve=reserve, selection=chosen)

    def resolve(reader, selected, selection, block):
        assert (selection.connector, selection.spoke) == ("aave_v4", usdc.spoke)
        return resolved(mainnet(weth, selection.reserve_id))

    monkeypatch.setattr(lending_lookup, "resolve", resolve)
    return lambda reserve_id=USDC: resolved(mainnet(usdc if reserve_id == USDC else weth))


def configured(document):
    """The deployment configuration the command validates the positions into."""
    limits = ax_actions.FeeLimits(max_gas_price_wei=5, max_transaction_fee=9)
    chain = Chain(chain_id=ETHEREUM, environment="mainnet")
    return ax_actions.configuration(
        "aave_v4", document, WALLET, ax_actions.alchemy(ETHEREUM), chain, limits
    )


def test_the_connector_declares_borrow_and_full_repayment_on_both_chains():
    assert binding("aave_v4").actions == ("supply", "withdraw", "borrow", "repay")
    cells = connector_from_wire({"connector": "aave_v4"}).chain_support
    assert [cell.chain for cell in cells] == [ETHEREUM, AVALANCHE]
    expected = tuple(EconomicAction(name) for name in ("supply", "withdraw", "borrow", "repay"))
    assert all(cell.direct_actions == cell.actions == expected for cell in cells)


def test_the_wallets_reserves_on_a_spoke_are_those_it_supplies_or_owes():
    reader = Spoke(
        r0={"supplied": 9, "collateral": True},
        r2={"supplied": 4},
        # Enabled with nothing supplied: it backs nothing.
        r3={"collateral": True},
        r5={"premium": 1},
        r6={"offset": -1},
        r7={"drawn": 3},
    )
    assert direct.held_reserves(reader, None, "spoke", WALLET.address) == {
        0: direct.Held(9, False, True),
        2: direct.Held(4, False, False),
        5: direct.Held(0, True, False),
        6: direct.Held(0, True, False),
        7: direct.Held(0, True, False),
    }
    assert direct.held_reserves(Spoke(), None, "spoke", WALLET.address) == {}


def test_a_borrow_binds_the_debt_with_every_supply_that_can_back_it(market):
    reader = Spoke(
        factors={0: 8300, 3: 0, 5: 0},
        r0={"supplied": 9},
        # Collateral already, whatever its factor is now.
        r3={"supplied": 4, "collateral": True},
        # No collateral factor and not collateral: the borrow leaves it as it is.
        r5={"supplied": 2},
    )
    document = direct.positions(reader, None, market(), WALLET, BORROW)
    assert [a["name"] for a in document["allocations"]] == ["collateral_0", "collateral_3"]
    assert [a["reserve"]["reserve_id"] for a in document["allocations"]] == [0, 3]
    (debt,) = document["debts"]
    assert (debt["name"], debt["reserve"]["reserve_id"]) == (ALLOCATION, USDC)
    assert debt["collateral"] == ["collateral_0", "collateral_3"]
    assert debt["minimum_health_factor"] == FLOOR
    # The Spoke oracle and the source it uses for each reserve now.
    assert debt["oracle"] == ORACLE and debt["debt_price_source"] == feed(USDC)
    assert debt["collateral_price_sources"] == [feed(0), feed(3)]

    config = configured(document)
    assert [a.name for a in config.positions] == ["collateral_0", "collateral_3", ALLOCATION]
    assert {a.account for a in config.positions} == {"treasury"}
    assert config.target(ALLOCATION).fund.address == WALLET.address
    capabilities = declared(config)
    borrow = capabilities.requirements[-1]
    assert (borrow.kind, borrow.target) == ("aave_borrow", config.target(ALLOCATION))
    assert borrow.collateral == (config.target("collateral_0"), config.target("collateral_3"))
    for request in (BORROW, REPAY):
        intent = direct.intent(config, (), request)
        assert (intent.kind, intent.target) == (request.kind, config.target(ALLOCATION))
        admit_intent(capabilities, intent)
    assert direct.intent(config, (), BORROW).amount.amount == 5
    # The debt position takes no supply, and the collateral positions no borrow.
    with pytest.raises(ValueError, match="outside its asset-specific capability"):
        admit_intent(capabilities, config.supply(ALLOCATION, 5))
    with pytest.raises(ValueError, match="outside its asset-specific capability"):
        admit_intent(capabilities, config.borrow("collateral_0", 5))


def test_a_repayment_binds_every_supply_and_states_the_lowest_floor(market):
    held = {"r0": {"supplied": 9, "collateral": True}, "r5": {"supplied": 2}, "r7": {"drawn": 3}}
    document = direct.positions(Spoke(**held), None, market(), WALLET, REPAY)
    (debt,) = document["debts"]
    # It enables nothing, so it binds the supply a borrow would leave out as well.
    assert debt["collateral"] == ["collateral_0", "collateral_5"]
    assert debt["collateral_price_sources"] == [feed(0), feed(5)]
    assert debt["minimum_health_factor"] == 10**18 + 1
    assert configured(document).debts[0].minimum_health_factor == 10**18 + 1
    # The command line takes no floor for a repayment; a stated one would be kept.
    assert direct.health_floor(ActionRequest("repay", min_health_factor=FLOOR)) == FLOOR
    assert direct.health_floor(BORROW) == FLOOR
    with pytest.raises(ValueError, match="borrow requires --min-health-factor"):
        direct.health_floor(ActionRequest("borrow", 5))


def repayment(proof):
    """The capture's four steps of one full repayment, in order."""
    return [
        o
        for o in proof.outcomes
        if o.decision.intent.kind == "repay" and o.before.target.allocation == DEBT
    ]


def test_no_floor_can_refuse_a_repayment(borrowing_proof, monkeypatch):  # noqa: F811
    """Only a borrow is projected against the floor: every repayment step plans and
    validates under a floor no position meets."""
    projected = []
    monkeypatch.setattr(aave_execution, "project_borrow", lambda *a: projected.append(a[3]))
    config = borrowing_proof.config
    debt = config.allocation(DEBT).model_copy(update={"minimum_health_factor": 10**30})
    state = SimpleNamespace(config=config, owned=borrowing_proof.owned, outcomes=(), mode="running")
    steps = repayment(borrowing_proof)
    assert [o.operation.kind for o in steps] == ["approve", "repay", "revoke", "collateral"]
    for outcome in steps:
        operation = plan_operation(config.repay(DEBT), outcome.before, debt)
        assert operation == outcome.operation
        state.owned = ownership_before(borrowing_proof, outcome)
        validate_debt_operation(state, operation, outcome.before, debt)
        backend = SimpleNamespace(config=config, snapshot=lambda name, block, o=outcome: o.before)
        plan = SimpleNamespace(operation=operation, call=outcome.call)
        aave_execution.current_debt_checks(backend, plan, outcome.before.block)
        assert aave_execution.borrow_projection(backend, operation, debt, None, None) is None
    assert projected == []
    # The repayment is complete only once the collateral it released is disabled.
    assert [o.strategy_result().complete for o in steps] == [False, False, False, True]
    borrow = next(o for o in borrowing_proof.outcomes if o.operation.kind == "borrow")
    backend = SimpleNamespace(config=config, reader=SimpleNamespace(pin_block=lambda number: None))
    aave_execution.borrow_projection(
        backend, borrow.operation, debt, borrow.call, borrow.before.block
    )
    assert projected == [borrow.call]


def ownership_before(proof, outcome):
    """Durable ownership as the capture's outcomes before `outcome` leave it."""
    owned = tuple(Ownership(target=item.target, shares=0) for item in proof.owned)
    for earlier in proof.outcomes:
        if earlier is outcome:
            return owned
        owned = commit_ownership(owned, earlier)
    raise AssertionError("outcome is not part of the capture")


def test_a_borrow_or_repayment_is_refused_without_a_position_to_bind(market):
    nothing = "has supplied no collateral on this Spoke"
    for held in (
        {},
        # A supply in the borrowed reserve itself is not another position to bind.
        {"r7": {"supplied": 5}},
        {"r5": {"supplied": 2}},
    ):
        with pytest.raises(ValueError, match=nothing):
            direct.positions(Spoke(factors={5: 0}, **held), None, market(), WALLET, BORROW)
    with pytest.raises(ValueError, match="supplies no reserve of this Spoke other than reserve 7"):
        direct.positions(Spoke(r7={"drawn": 3}), None, market(), WALLET, REPAY)


def test_a_second_debt_reserve_on_the_spoke_is_refused(market):
    reader = Spoke(r0={"supplied": 9, "collateral": True}, r3={"drawn": 3}, r4={"premium": 1})
    for request in (BORROW, REPAY):
        with pytest.raises(ValueError, match="has debt in reserve 3 of this Spoke") as refused:
            direct.positions(reader, None, market(), WALLET, request)
        assert "one debt reserve per wallet and Spoke: repay reserve 3 in full" in str(
            refused.value
        )
    # More of the same debt is the one debt reserve.
    same = Spoke(factors={}, r0={"supplied": 9, "collateral": True}, r7={"drawn": 3})
    assert direct.positions(same, None, market(), WALLET, BORROW)["debts"][0]["name"] == ALLOCATION


def test_collateral_is_not_withdrawn_while_the_wallet_owes_on_the_spoke(market):
    backing = Spoke(r0={"supplied": 9, "collateral": True}, r2={"supplied": 4}, r7={"drawn": 3})
    with pytest.raises(
        ValueError, match="reserve 0 is collateral for the wallet's debt"
    ) as refused:
        direct.positions(backing, None, market(WETH), WALLET, WITHDRAW)
    assert "in reserve 7 of this Spoke: repay that debt in full first" in str(refused.value)
    # A supply that is not collateral, or collateral with no debt open, backs nothing.
    released = Spoke(r0={"supplied": 9, "collateral": True})
    unused = Spoke(r0={"supplied": 9}, r3={"supplied": 4, "collateral": True}, r7={"drawn": 3})
    for reader in (released, unused, Spoke()):
        document = direct.positions(reader, None, market(WETH), WALLET, WITHDRAW)
        assert [a["name"] for a in document["allocations"]] == [ALLOCATION]
        assert "debts" not in document


def test_a_supply_binds_its_reserve_without_reading_the_spoke(market):
    document = direct.positions(None, None, market(WETH), WALLET, ActionRequest("supply", 5))
    (allocation,) = document["allocations"]
    assert (allocation["name"], allocation["reserve"]["reserve_id"]) == (ALLOCATION, WETH)
    assert configured(document).debts == ()


def test_a_reserve_the_wallet_owes_takes_no_supply(borrowing_proof):  # noqa: F811
    """A supply position with observed debt is refused when its first step is planned."""
    config = borrowing_proof.config
    borrow = next(o for o in borrowing_proof.outcomes if o.operation.kind == "borrow")
    before = next(o for o in borrow.decision.observations if o.allocation == COLLATERAL)
    owed = before.position.model_copy(update={"drawn_debt": 1, "total_debt": 1})
    before = before.model_copy(update={"position": owed})
    intent = config.supply(COLLATERAL, 5)
    state = borrowing_proof.model_copy(
        update={
            "mode": "running",
            "owned": ownership_before(borrowing_proof, borrow),
            "decision": borrow.decision.model_copy(update={"intent": intent}),
        }
    )
    gateway = SimpleNamespace(config=config, guard=lambda: None)
    with pytest.raises(DataError, match="observed exposure differs from owned supply") as refused:
        aave_execution.validate_operation(gateway, state, plan_operation(intent, before), before)
    assert refused.value.kind == "ownership_mismatch"


def test_adoption_takes_supply_and_debt_ownership_from_the_observed_positions(
    borrowing_proof,  # noqa: F811
):
    approve = repayment(borrowing_proof)[0]
    observed = tuple(o for o in approve.decision.observations if o.allocation in (COLLATERAL, DEBT))
    empty = tuple(Ownership(target=o.target, shares=0) for o in observed)
    collateral, debt = adopt_ownership(empty, observed)
    assert collateral.shares == observed[0].position.supplied_shares > 0
    assert collateral.collateral_enabled and not collateral.drawn_shares
    assert debt.drawn_shares == observed[1].drawn_shares > 0 and debt.shares == 0
    # It is the ownership the capture's own outcomes committed, step by step.
    committed = {o.target: o for o in ownership_before(borrowing_proof, approve)}
    assert (collateral, debt) == (committed[collateral.target], committed[debt.target])
    for owned, seen in zip((collateral, debt), observed, strict=True):
        validate_owned(owned, seen)
    with pytest.raises(ValueError):
        adopt_ownership(empty[:1], observed)


def operator(proof, outcome):
    """An operator deployment of the capture's positions, about to plan `outcome`."""
    return proof.model_copy(
        update={
            "operator": True,
            "strategy": None,
            "mode": "running",
            "outcomes": (),
            "delivered": 0,
            "closure": (),
            "owned": ownership_before(proof, outcome),
            "decision": outcome.decision,
        }
    )


def test_the_gateway_admits_every_step_of_an_operator_borrow_and_repayment(
    borrowing_proof,  # noqa: F811
):
    """The execution adapter's own checks, with the direct declaration it enforces."""
    steps = [
        o
        for o in borrowing_proof.outcomes
        if o.decision.intent.kind in ("borrow", "repay") and o.before.target.allocation == DEBT
    ]
    kinds = ["collateral", "borrow", "approve", "repay", "revoke", "collateral"]
    assert [o.operation.kind for o in steps] == kinds
    gateway = object.__new__(ForkGateway)
    gateway.config, gateway.guard = borrowing_proof.config, lambda: None
    gateway.reader = SimpleNamespace(chain=steps[0].operation.target.fund.chain)
    for outcome in steps:
        gateway.validate_operation(
            operator(borrowing_proof, outcome), outcome.operation, outcome.before
        )


def test_the_result_reports_the_requested_position_by_name():
    def owned(name, shares, drawn):
        target = SimpleNamespace(allocation=name)
        return SimpleNamespace(target=target, shares=shares, drawn_shares=drawn)

    state = SimpleNamespace(owned=(owned("collateral_0", 9, 0), owned(ALLOCATION, 0, 4)))
    assert direct.position(state) == {"supplied_shares": "0", "debt_shares": "4"}


def test_an_action_states_its_amount_exactly_once(borrowing_proof, monkeypatch):  # noqa: F811
    monkeypatch.setattr(direct, "ALLOCATION", DEBT)
    config = borrowing_proof.config
    assert direct.intent(config, (), BORROW).target == config.target(DEBT)
    assert direct.intent(config, (), REPAY).kind == "repay"
    with pytest.raises(ValueError, match="borrow requires --amount"):
        direct.intent(config, (), ActionRequest("borrow"))
    with pytest.raises(ValueError, match="repay takes no --amount: it repays the whole debt"):
        direct.intent(config, (), ActionRequest("repay", 5))
    with pytest.raises(ValueError, match="withdraw takes no --amount: it withdraws the whole"):
        direct.intent(config, (), ActionRequest("withdraw", 5))


COMMAND = [
    "--connector",
    "aave_v4",
    "--chain",
    "eip155:1",
    "--spoke",
    "0x94e7a5dcbe816e498b89ab752661904e2f56c485",
    "--reserve-id",
    "7",
    "--wallet",
    "wallet",
    "--env-file",
    ".env",
    "--max-gas-price-wei",
    "7",
    "--max-transaction-fee",
    "9",
]


def test_a_partial_repayment_or_withdrawal_is_refused_before_any_read(monkeypatch):
    def unreachable(*arguments):
        raise AssertionError("the command read before refusing")

    monkeypatch.setattr(ax_actions, "read_wallet", unreachable)
    monkeypatch.setattr(ax_actions, "transport", unreachable)
    for action in ("repay", "withdraw"):
        arguments = parse_validated(build_parser(), ["ax", action, *COMMAND, "--amount", "5"])
        refusal = f"aave_v4 serves no direct {action}_amount on eip155:1"
        with pytest.raises(ValueError, match=refusal):
            ax_actions.command(arguments)
        for chain in (ETHEREUM, AVALANCHE):
            require_direct("aave_v4", chain, ActionRequest(action))
            with pytest.raises(ValueError, match=f"no direct {action}_amount on {chain}"):
                require_direct("aave_v4", chain, ActionRequest(action, 5))
    with pytest.raises(ValueError, match="aave_v4 serves no direct collateral"):
        require_direct("aave_v4", ETHEREUM, ActionRequest("collateral", enable=False))


def test_a_borrow_states_its_amount_and_floor_and_a_repayment_neither(capsys):
    borrow = parse_validated(
        build_parser(),
        ["ax", "borrow", *COMMAND, "--amount", "5", "--min-health-factor", "1.5"],
    )
    assert (borrow.amount, borrow.min_health_factor) == (5, FLOOR)
    repay = parse_validated(build_parser(), ["ax", "repay", *COMMAND])
    assert repay.amount is None and not hasattr(repay, "min_health_factor")
    for arguments in (
        ["borrow", *COMMAND, "--amount", "5"],
        ["borrow", *COMMAND, "--min-health-factor", "1.5"],
        ["repay", *COMMAND, "--min-health-factor", "1.5"],
    ):
        with pytest.raises(SystemExit):
            parse_validated(build_parser(), ["ax", *arguments])
    capsys.readouterr()
