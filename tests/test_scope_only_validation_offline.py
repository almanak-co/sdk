"""Scope-only gateway admission for Aerodrome, Curve, Comet and Morpho, without evidence.

Blueprint 27 removes author amount bounds: authority is the retained target and
liquidity scope, and the gateway checks inventory, quotes, slippage, deadlines and
entry eligibility against observations. These cases drive each connector's
`validate_operation` with literal observations and stub only the collaborators
that read protocol state (ownership, planning replay and capacity projection).
"""

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.caip import AssetType
from almanak.connectors.aerodrome import authority as aerodrome_authority
from almanak.connectors.aerodrome import operations as aerodrome_ops
from almanak.connectors.aerodrome import validation as aerodrome
from almanak.connectors.compound_v3 import domain as compound_domain
from almanak.connectors.compound_v3 import execution as compound
from almanak.connectors.compound_v3 import operations as compound_ops
from almanak.connectors.curve import authority as curve_authority
from almanak.connectors.curve import operations as curve_ops
from almanak.connectors.curve import permissions as curve_permissions
from almanak.connectors.curve import validation as curve
from almanak.connectors.morpho_blue import domain as morpho_domain
from almanak.connectors.morpho_blue import execution as morpho
from almanak.connectors.morpho_blue import operations as morpho_ops
from almanak.dex_intents import (
    CloseLiquidity,
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    ExitExchange,
    OpenLiquidity,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError
from almanak.lending_intents import RepayIntent
from almanak.models import AssetAmount

A = AssetType("eip155:8453/erc20:0x" + "aa" * 20)
B = AssetType("eip155:8453/erc20:0x" + "bb" * 20)
NATIVE = AssetType("eip155:8453/slip44:60")
NOW = 1_000
ALLOCATION = SimpleNamespace(deadline_seconds=600, slippage_bps=100, minimum_native_balance=5)


def amount(asset, value):
    return SimpleNamespace(asset=asset, amount=value)


def refusal(call):
    with pytest.raises(DataError) as refused:
        call()
    return refused.value.kind


# Aerodrome and Curve share the gateway sequence and differ in observation shape.


class Venue:
    def __init__(self, name):
        self.name = name
        self.module = aerodrome if name == "aerodrome" else curve
        self.ops = aerodrome_ops if name == "aerodrome" else curve_ops
        funding = "funding_currency" if name == "aerodrome" else "funding_asset"
        self.target = SimpleNamespace(
            allocation="pool", liquidity_enabled=True, single_coin_exit=None, **{funding: A}
        )

    def swap(self, value=50, minimum=99, *, asset=A, output=B, deadline=NOW + 300):
        return SwapExactInput.model_construct(
            target=self.target,
            amount=amount(asset, value),
            minimum_output=amount(output, minimum),
            deadline=deadline,
        )

    def open(self):
        if self.name == "aerodrome":
            return OpenLiquidity.model_construct(
                target=self.target, maximum0=amount(A, 10), maximum1=amount(B, 10), deadline=NOW + 1
            )
        return OpenPoolLiquidity.model_construct(
            target=self.target,
            maximums=SimpleNamespace(root=(amount(A, 10), amount(B, 10))),
            deadline=NOW + 1,
        )

    def quote(self, intent, out=100):
        calculation = "contract_router" if self.name == "aerodrome" else "contract_pool"
        return SimpleNamespace(
            amount_in=intent.amount,
            amount_out=amount(intent.minimum_output.asset, out),
            calculation=calculation,
        )

    def basis(self, quote=None, simulated=False):
        exits: dict[str, Any] = (
            {"exit_quote": None} if self.name == "aerodrome" else {"exit_quotes": ()}
        )
        return SimpleNamespace(simulated=simulated, entry_quote=quote, **exits)

    def before(self, *, native=10, inventory=(100, 100)):
        values = (amount(A, inventory[0]), amount(B, inventory[1]))
        if self.name == "aerodrome":
            owned = SimpleNamespace(inventory0=values[0], inventory1=values[1], position=None)
            extra: dict[str, Any] = {"exit_quote": None}
        else:
            owned = SimpleNamespace(inventory=SimpleNamespace(root=values), position=None)
            # A Curve refusal for time names the block and the allowances that stand.
            extra = {"exit_quotes": (), "allowances": ()}
        return SimpleNamespace(
            simulated=False,
            target=self.target,
            block=SimpleNamespace(timestamp=NOW, number=1),
            owned=owned,
            pool=SimpleNamespace(paused=False),
            native_balance=amount(NATIVE, native),
            **extra,
        )

    def swap_operation(self, intent):
        return self.ops.Swap.model_construct(
            target=self.target,
            amount=intent.amount,
            minimum=intent.minimum_output,
            deadline=intent.deadline,
        )

    def approval(self, asset, value):
        return self.ops.Approval.model_construct(
            target=self.target, asset=asset, spender="0x" + "cc" * 20, amount=value
        )


@pytest.fixture(params=["aerodrome", "curve"])
def venue(request):
    return Venue(request.param)


def dex_state(venue, intent, *, mode="running", capabilities=True, target=None):
    requirements = (SimpleNamespace(target=venue.target),) if capabilities else ()
    bound = target or venue.target
    return SimpleNamespace(
        decision=SimpleNamespace(intent=intent),
        capabilities=SimpleNamespace(requirements=requirements),
        config=SimpleNamespace(target=lambda name: bound, allocation=lambda name: ALLOCATION),
        owned=(SimpleNamespace(target=venue.target),),
        mode=mode,
        outcomes=(),
    )


def install(monkeypatch, venue, basis, planned=None):
    seen = []
    monkeypatch.setattr(venue.module, "validate_owned", lambda owned, before: seen.append(owned))
    monkeypatch.setattr(venue.module.planning, "decision_basis", lambda decision, target: basis)
    monkeypatch.setattr(
        venue.module.planning, "operation", lambda config, state, intent, before: planned
    )
    return seen


def test_dex_swap_is_admitted_by_target_scope_quote_and_inventory(monkeypatch, venue):
    intent = venue.swap()
    operation = venue.swap_operation(intent)
    state = dex_state(venue, intent)
    seen = install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
    venue.module.validate_operation(None, state, operation, venue.before())
    assert seen == [state.owned[0]]


def test_dex_swap_refuses_missing_authority_and_unbound_targets(monkeypatch, venue):
    intent = venue.swap()
    operation = venue.swap_operation(intent)
    install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
    validate = venue.module.validate_operation
    simulated = venue.before()
    simulated.simulated = True
    with pytest.raises(ValueError, match="on-chain inputs"):
        validate(None, dex_state(venue, intent), operation, simulated)
    missing = dex_state(venue, intent)
    missing.capabilities = None
    with pytest.raises(ValueError, match="on-chain inputs"):
        validate(None, missing, operation, venue.before())
    undecided = dex_state(venue, intent)
    undecided.decision = None
    with pytest.raises(ValueError, match="on-chain inputs"):
        validate(None, undecided, operation, venue.before())
    with pytest.raises(ValueError, match="one exact retained target capability"):
        validate(None, dex_state(venue, intent, capabilities=False), operation, venue.before())
    other = SimpleNamespace(**{**vars(venue.target), "allocation": "other"})
    with pytest.raises(ValueError, match="bound target"):
        validate(None, dex_state(venue, intent, target=other), operation, venue.before())


def test_dex_swap_refuses_simulated_decisions_and_replay_differences(monkeypatch, venue):
    intent = venue.swap()
    operation = venue.swap_operation(intent)
    validate = venue.module.validate_operation
    install(monkeypatch, venue, venue.basis(venue.quote(intent), simulated=True), operation)
    with pytest.raises(ValueError, match="simulated decisions"):
        validate(None, dex_state(venue, intent), operation, venue.before())
    widened = venue.swap_operation(venue.swap(minimum=1))
    install(monkeypatch, venue, venue.basis(venue.quote(intent)), widened)
    with pytest.raises(ValueError, match="differs from its retained economic intent"):
        validate(None, dex_state(venue, intent), operation, venue.before())


def test_dex_swap_refuses_stale_deadlines_inventory_and_slippage(monkeypatch, venue):
    validate = venue.module.validate_operation
    for intent, kind in (
        (venue.swap(deadline=NOW), "stale_plan"),
        (venue.swap(deadline=NOW + 601), "stale_plan"),
        (venue.swap(value=101), "inventory"),
    ):
        operation = venue.swap_operation(intent)
        install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
        assert (
            refusal(
                lambda intent=intent, operation=operation: validate(
                    None, dex_state(venue, intent), operation, venue.before()
                )
            )
            == kind
        )
    intent = venue.swap(minimum=98)
    operation = venue.swap_operation(intent)
    install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
    with pytest.raises(ValueError, match="widens the configured slippage"):
        validate(None, dex_state(venue, intent), operation, venue.before())
    install(monkeypatch, venue, venue.basis(None), operation)
    with pytest.raises(ValueError, match="exact-size retained"):
        validate(None, dex_state(venue, intent), operation, venue.before())


def test_dex_entries_need_a_running_deployment_and_payer_minimum(monkeypatch, venue):
    intent = venue.swap()
    operation = venue.swap_operation(intent)
    install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
    validate = venue.module.validate_operation
    paused = dex_state(venue, intent, mode="paused")
    assert refusal(lambda: validate(None, paused, operation, venue.before())) == "accounting"
    low = venue.before(native=4)
    assert refusal(lambda: validate(None, dex_state(venue, intent), operation, low)) == "gas_budget"
    unobserved = venue.before()
    unobserved.native_balance = None
    assert (
        refusal(lambda: validate(None, dex_state(venue, intent), operation, unobserved))
        == "gas_budget"
    )


def test_dex_exits_and_swaps_back_to_funding_are_not_entry_gated(monkeypatch, venue):
    validate = venue.module.validate_operation
    back = venue.swap(asset=B, output=A)
    operation = venue.swap_operation(back)
    install(monkeypatch, venue, venue.basis(venue.quote(back)), operation)
    validate(None, dex_state(venue, back, mode="paused"), operation, venue.before(native=0))
    intent: Any
    for intent in (
        ExitExchange.model_construct(target=venue.target),
        RevokeExchange.model_construct(target=venue.target),
    ):
        revoke = venue.approval(A, 0)
        install(monkeypatch, venue, venue.basis(), revoke)
        validate(None, dex_state(venue, intent, mode="paused"), revoke, venue.before(native=0))


def test_dex_liquidity_entries_need_liquidity_scope_and_entry_eligibility(monkeypatch, venue):
    validate = venue.module.validate_operation
    intent = venue.open()
    # The non-funding approval does not itself add exposure; the liquidity intent does.
    operation = venue.approval(B, 10)
    install(monkeypatch, venue, venue.basis(), operation)
    limits = []
    monkeypatch.setattr(
        venue.module, "addition_limits", lambda *args: limits.append(args[0] is intent)
    )
    validate(None, dex_state(venue, intent), operation, venue.before())
    assert limits == [True]
    paused = dex_state(venue, intent, mode="paused")
    assert refusal(lambda: validate(None, paused, operation, venue.before())) == "accounting"
    venue.target.liquidity_enabled = False
    with pytest.raises(ValueError, match="liquidity authority"):
        validate(None, dex_state(venue, intent), operation, venue.before())


def test_aerodrome_swaps_refuse_a_paused_pool(monkeypatch):
    venue = Venue("aerodrome")
    intent = venue.swap()
    operation = venue.swap_operation(intent)
    install(monkeypatch, venue, venue.basis(venue.quote(intent)), operation)
    before = venue.before()
    before.pool.paused = True
    assert (
        refusal(
            lambda: aerodrome.validate_operation(None, dex_state(venue, intent), operation, before)
        )
        == "market_paused"
    )


def capabilities(*targets):
    return SimpleNamespace(requirements=tuple(SimpleNamespace(target=t) for t in targets))


def test_aerodrome_admission_binds_one_target_and_liquidity_scope():
    target = SimpleNamespace(liquidity_enabled=True)
    close: Any = CloseLiquidity.model_construct(target=target)
    with pytest.raises(ValueError, match="retained capabilities"):
        aerodrome_authority.admit(None, close)
    with pytest.raises(ValueError, match="one exact retained"):
        aerodrome_authority.admit(capabilities(target, target), close)
    aerodrome_authority.admit(capabilities(target), close)
    with pytest.raises(ValueError, match="unsupported Aerodrome"):
        aerodrome_authority.admit(
            capabilities(target), ClosePoolLiquidity.model_construct(target=target)
        )
    disabled = SimpleNamespace(liquidity_enabled=False)
    aerodrome_authority.admit(capabilities(disabled), ExitExchange.model_construct(target=disabled))
    with pytest.raises(ValueError, match="pool liquidity authority"):
        aerodrome_authority.admit(
            capabilities(disabled), CloseLiquidity.model_construct(target=disabled)
        )


def test_curve_admission_binds_one_target_and_single_coin_exit():
    target = SimpleNamespace(liquidity_enabled=True, single_coin_exit=A)
    with pytest.raises(ValueError, match="retained capabilities"):
        curve_authority.admit(None, ExitExchange.model_construct(target=target))
    with pytest.raises(ValueError, match="one exact retained"):
        curve_authority.admit(capabilities(), ExitExchange.model_construct(target=target))
    for output in (None, A):
        curve_authority.admit(
            capabilities(target),
            DecreasePoolLiquidity.model_construct(target=target, output_asset=output),
        )
    with pytest.raises(ValueError, match="single-coin removal"):
        curve_authority.admit(
            capabilities(target), ClosePoolLiquidity.model_construct(target=target, output_asset=B)
        )
    with pytest.raises(ValueError, match="unsupported Curve"):
        curve_authority.admit(capabilities(target), CloseLiquidity.model_construct(target=target))
    disabled = SimpleNamespace(liquidity_enabled=False, single_coin_exit=None)
    curve_authority.admit(capabilities(disabled), SwapExactInput.model_construct(target=disabled))
    with pytest.raises(ValueError, match="pool liquidity authority"):
        curve_authority.admit(
            capabilities(disabled), OpenPoolLiquidity.model_construct(target=disabled)
        )


def test_curve_permissions_grant_every_coin_and_only_configured_liquidity(monkeypatch):
    fund, other = "fund", "other"
    calls = []
    for name in ("approval_grant", "swap_grant"):
        monkeypatch.setattr(
            curve_permissions, name, lambda target, value, name=name: (name, target.name, value)
        )
    for name in ("addition_grant", "proportional_grant", "single_coin_grant"):
        monkeypatch.setattr(curve_permissions, name, lambda target, name=name: (name, target.name))

    def combine(grants):
        calls.append(grants)
        return grants

    monkeypatch.setattr(curve_permissions, "combine", combine)

    def target(name, owner, liquidity, single):
        return SimpleNamespace(
            name=name,
            fund=owner,
            assets=(A, B),
            liquidity_enabled=liquidity,
            single_coin_exit=single,
        )

    granted = curve_permissions.derive(
        SimpleNamespace(fund=fund),
        capabilities(
            target("swap", fund, False, None),
            target("lp", fund, True, None),
            target("single", fund, True, A),
            target("foreign", other, True, A),
        ),
    )
    assert calls == [granted]
    assert granted == [
        ("approval_grant", "swap", A),
        ("swap_grant", "swap", 0),
        ("approval_grant", "swap", B),
        ("swap_grant", "swap", 1),
        ("approval_grant", "lp", A),
        ("swap_grant", "lp", 0),
        ("approval_grant", "lp", B),
        ("swap_grant", "lp", 1),
        ("addition_grant", "lp"),
        ("proportional_grant", "lp"),
        ("approval_grant", "single", A),
        ("swap_grant", "single", 0),
        ("approval_grant", "single", B),
        ("swap_grant", "single", 1),
        ("addition_grant", "single"),
        ("proportional_grant", "single"),
        ("single_coin_grant", "single"),
    ]


# Comet and Morpho share the guarded lending sequence.

USDC = AssetType("eip155:1/erc20:0x" + "a0" * 20)


class Lending:
    def __init__(self, name):
        self.name = name
        self.module: Any
        self.ops: Any
        self.target: Any
        chain = SimpleNamespace(chain_id=USDC.chain)
        fund = SimpleNamespace(chain=chain)
        if name == "compound":
            self.module, self.ops = compound, compound_ops
            reserve = SimpleNamespace(asset=USDC, chain=chain, is_base=True)
            self.target = compound_domain.Target.model_construct(
                allocation="loan", fund=fund, reserve=reserve
            )
        else:
            self.module, self.ops = morpho, morpho_ops
            reserve = SimpleNamespace(asset=USDC, chain=chain, is_loan=True)
            self.target = morpho_domain.Target.model_construct(
                allocation="loan", fund=fund, reserve=reserve
            )

    def value(self, units=10):
        return AssetAmount(asset=USDC, amount=units)


@pytest.fixture(params=["compound", "morpho"])
def lending(request):
    return Lending(request.param)


class Observed(SimpleNamespace):
    def model_copy(self, update):
        return Observed(**{**vars(self), **update})


def lending_before(lending, *, allowance=0, native=10):
    return Observed(
        target=lending.target,
        allowance=allowance,
        native_balance=SimpleNamespace(amount=native),
        account=SimpleNamespace(position=SimpleNamespace(borrow_shares=77)),
    )


def lending_state(lending, intent=None, *, mode="running", baseline=(), outcomes=()):
    return SimpleNamespace(
        mode=mode,
        decision=SimpleNamespace(intent=intent),
        owned=(SimpleNamespace(target=lending.target),),
        outcomes=outcomes,
        baseline=baseline,
        config=SimpleNamespace(allocation=lambda name: ALLOCATION),
    )


def lending_install(monkeypatch, lending, planned):
    seen = SimpleNamespace(guard=0, ownership=[], market=[], capacity=[])
    backend = SimpleNamespace(config="config", guard=lambda: setattr(seen, "guard", seen.guard + 1))
    module = lending.module
    monkeypatch.setattr(module.planning, "operation", lambda config, state, intent, before: planned)
    monkeypatch.setattr(
        module.planning,
        "validate_ownership",
        lambda retained, before: seen.ownership.append(retained),
    )
    monkeypatch.setattr(
        module, "validate_market_ownership", lambda state, before: seen.market.append(state)
    )
    monkeypatch.setattr(
        module.capacity,
        "validate",
        lambda config, before, core: seen.capacity.append((before, core)),
    )
    return backend, seen


def test_lending_supply_is_admitted_through_capacity_without_an_amount_bound(monkeypatch, lending):
    supply = lending.ops.SupplyOperation(target=lending.target, amount=lending.value(10**30))
    backend, seen = lending_install(monkeypatch, lending, supply)
    before = lending_before(lending)
    state = lending_state(lending)
    lending.module.validate_operation(backend, state, supply, before)
    assert seen.guard == 1
    assert seen.ownership == [state.owned[0]] and seen.market == [state]
    assert seen.capacity == [(before, supply)]


def test_lending_supply_approval_projects_the_admitted_allowance(monkeypatch, lending):
    approval = lending.ops.ApproveOperation(
        target=lending.target, amount=lending.value(), purpose="supply"
    )
    backend, seen = lending_install(monkeypatch, lending, approval)
    before = lending_before(lending)
    lending.module.validate_operation(backend, lending_state(lending), approval, before)
    ((projected, core),) = seen.capacity
    assert before.allowance == 0 and projected.allowance == 10
    assert core == lending.ops.SupplyOperation(target=lending.target, amount=lending.value())


def test_lending_full_repay_approval_is_not_entry_gated(monkeypatch, lending):
    approval = lending.ops.ApproveOperation(
        target=lending.target, amount=lending.value(), purpose="repay"
    )
    backend, seen = lending_install(monkeypatch, lending, approval)
    intent: Any = RepayIntent.model_construct(target=lending.target)
    state = lending_state(lending, intent, mode="paused")
    lending.module.validate_operation(backend, state, approval, lending_before(lending, native=0))
    ((_, core),) = seen.capacity
    assert isinstance(core, lending.ops.RepayOperation)
    assert core.full and core.amount == lending.value()
    if lending.name == "morpho":
        assert core.shares == 77


def test_lending_refuses_closed_deployments_and_replay_differences(monkeypatch, lending):
    supply = lending.ops.SupplyOperation(target=lending.target, amount=lending.value())
    backend, _ = lending_install(monkeypatch, lending, supply)
    validate = lending.module.validate_operation
    with pytest.raises(ValueError, match="closed deployments"):
        validate(backend, lending_state(lending, mode="closed"), supply, lending_before(lending))
    undecided = lending_state(lending)
    undecided.decision = None
    with pytest.raises(ValueError, match="retained economic decision"):
        validate(backend, undecided, supply, lending_before(lending))
    other = lending.ops.SupplyOperation(target=lending.target, amount=lending.value(11))
    backend, _ = lending_install(monkeypatch, lending, other)
    with pytest.raises(ValueError, match="retained economic decision"):
        validate(backend, lending_state(lending), supply, lending_before(lending))


def test_lending_allowance_must_match_the_latest_retained_value(monkeypatch, lending):
    supply = lending.ops.SupplyOperation(target=lending.target, amount=lending.value())
    backend, seen = lending_install(monkeypatch, lending, supply)
    validate = lending.module.validate_operation
    baseline = (SimpleNamespace(target=lending.target, allowance=5),)
    state = lending_state(lending, baseline=baseline)
    assert refusal(lambda: validate(backend, state, supply, lending_before(lending))) == (
        "allowance_mismatch"
    )
    later = (
        SimpleNamespace(
            after=SimpleNamespace(target=lending.target, allowance=0), accounting_issues=()
        ),
        SimpleNamespace(
            after=SimpleNamespace(target="elsewhere", allowance=9), accounting_issues=()
        ),
    )
    state = lending_state(lending, baseline=baseline, outcomes=later)
    validate(backend, state, supply, lending_before(lending))
    assert len(seen.capacity) == 1


def test_lending_entries_need_running_reconciled_mode_and_payer_minimum(monkeypatch, lending):
    supply = lending.ops.SupplyOperation(target=lending.target, amount=lending.value())
    backend, _ = lending_install(monkeypatch, lending, supply)
    validate = lending.module.validate_operation
    paused = lending_state(lending, mode="paused")
    assert refusal(lambda: validate(backend, paused, supply, lending_before(lending))) == (
        "entry_unavailable"
    )
    issue = (
        SimpleNamespace(after=SimpleNamespace(target="x", allowance=0), accounting_issues=(1,)),
    )
    unreconciled = lending_state(lending, outcomes=issue)
    assert refusal(lambda: validate(backend, unreconciled, supply, lending_before(lending))) == (
        "entry_unavailable"
    )
    low = lending_before(lending, native=4)
    assert refusal(lambda: validate(backend, lending_state(lending), supply, low)) == (
        "payer_unavailable"
    )


def test_lending_approvals_and_revocations_check_the_residual_allowance(monkeypatch, lending):
    baseline = (SimpleNamespace(target=lending.target, allowance=3),)
    approval = lending.ops.ApproveOperation(
        target=lending.target, amount=lending.value(), purpose="supply"
    )
    backend, seen = lending_install(monkeypatch, lending, approval)
    validate = lending.module.validate_operation
    held = lending_before(lending, allowance=3)
    assert refusal(
        lambda: validate(backend, lending_state(lending, baseline=baseline), approval, held)
    ) == ("allowance_mismatch")
    revoke = lending.ops.RevokeOperation(target=lending.target)
    backend, seen = lending_install(monkeypatch, lending, revoke)
    paused = lending_state(lending, mode="paused", baseline=baseline)
    validate(backend, paused, revoke, lending_before(lending, allowance=3, native=0))
    assert seen.capacity == []
    assert refusal(
        lambda: validate(backend, lending_state(lending), revoke, lending_before(lending))
    ) == ("allowance_mismatch")
