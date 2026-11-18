"""Both transports preserve captured debt economics and payer-scoped authority."""

import json
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_account import Account as Signer

from almanak.caip import AssetType
from almanak.connectors.aave_v4.deployments import validate_markets
from almanak.connectors.aave_v4.execution import validate_current_position
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.connectors.funding import (
    admission_reserve,
    native_entry_minimum,
    payer_accounts,
    payer_entry_transactions,
)
from almanak.gateway.accounts import prepare_call, protocol_result
from almanak.gateway.credentials import decrypt_signer
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.production import ProductionGateway
from almanak.gateway.production_fees import increases_exposure, validate_budget
from almanak.models import AssetAmount
from tests.test_aave_debt import borrowing_proof as borrowing_proof
from tests.test_production import ROOT, TEST_KEY, Reader, mainnet_value
from tests.test_runtime_recovery import pending_for


@pytest.fixture
def production_debt(borrowing_proof):
    raw = mainnet_value(borrowing_proof.model_dump(mode="json"))
    config = raw["config"]
    del config["fork_bindings"]
    policy = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/production.example.json").read_text()
    )["production"]
    policy["gas"][0]["accounts"] = [b["name"] for b in config["accounts"]["bindings"]]
    config["production"] = policy
    return Checkpoint.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("profile", [ForkGateway, ProductionGateway])
def test_profiles_reconcile_all_captured_borrowing_effects(
    profile, borrowing_proof, production_debt
):
    state = production_debt if profile is ProductionGateway else borrowing_proof
    backend = object.__new__(profile)
    backend.config, backend.guard = state.config, lambda: None
    for outcome in state.outcomes:
        pending = captured_pending(state, outcome)
        backend.reader = Reader(outcome.after.position.reserve.block)
        backend.snapshot = lambda *_, after=outcome.after: after
        assert backend.reconcile(pending, outcome.receipt) == outcome.model_copy(
            update={"call": pending.call, "execution": pending.execution}
        )


def captured_pending(state, outcome):
    pending = pending_for(state.config, outcome)
    return pending.model_copy(
        update={
            "execution": prepare_call(
                state.config.accounts, outcome.operation.account, pending.call
            ),
            "borrow_projection": outcome.borrow_projection,
        }
    )


def test_accrual_preserves_preconditions_but_changed_debt_shares_do_not(production_debt):
    before = next(o.after for o in production_debt.outcomes if o.operation.kind == "borrow")
    position = before.position.model_copy(
        update={
            "drawn_debt": before.position.drawn_debt + 7,
            "total_debt": before.position.total_debt + 7,
        }
    )
    current = before.model_copy(update={"position": position})
    validate_current_position(before, current)
    with pytest.raises(DataError, match="position"):
        validate_current_position(
            before, current.model_copy(update={"drawn_shares": before.drawn_shares + 1})
        )


def test_safe_repayment_simulates_full_drawn_shares_and_operator_envelope(production_debt):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    pending = captured_pending(production_debt, outcome)
    backend = object.__new__(ProductionGateway)
    backend.config = production_debt.config
    backend.reader = Reader(outcome.before.position.reserve.block)
    backend.reader.responses["eth_call"] = (
        "0x"
        + encode(
            ("bool", "bytes"),
            (True, encode(("uint256", "uint256"), (outcome.shares, outcome.assets))),
        ).hex()
    )
    backend.simulate_current(pending, pending.before)
    inner, outer = [params[0] for method, params in backend.reader.calls if method == "eth_call"]
    assert inner["from"] == pending.execution.submitter.address
    assert inner["to"] == pending.execution.to
    assert inner["data"][10:] == outer["data"][10:]
    assert inner["data"][:10] != outer["data"][:10]
    assert (outer["from"], outer["to"], outer["gas"]) == (
        pending.execution.submitter.address,
        pending.execution.to,
        hex(pending.gas),
    )
    assert inner["gasPrice"] == "0x0"


@pytest.mark.parametrize(
    "result,error",
    [
        ("0x", "malformed"),
        ("0x" + encode(("bool", "bytes"), (False, b"")).hex(), "successful"),
        ("0x" + encode(("bool", "bytes"), (True, b"")).hex() + "00", "successful"),
    ],
)
def test_safe_simulation_rejects_failed_or_ambiguous_return_data(production_debt, result, error):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    pending = captured_pending(production_debt, outcome)
    with pytest.raises(DataError, match=error):
        protocol_result(pending.execution, result)


def test_borrow_entry_protects_exit_budget_and_repayment_can_use_it(production_debt):
    expected = {"supply": True, "borrow": True, "repay": False, "withdraw": False, "revoke": False}
    for outcome in production_debt.outcomes:
        operation = outcome.operation
        if operation.kind in expected:
            assert increases_exposure(production_debt.config, operation) is expected[operation.kind]
        elif operation.kind == "collateral":
            assert increases_exposure(production_debt.config, operation) is operation.enabled
    repay_approval = next(
        o
        for o in production_debt.outcomes
        if o.operation.kind == "approve" and o.decision.intent.kind == "repay"
    )
    assert not increases_exposure(production_debt.config, repay_approval.operation)


def admitted_sequence(state, operations, policy, balance):
    """Admit each transaction at the fee ceiling, spending its fee, whatever it adds."""
    gas, price = policy.max_gas_limit, policy.max_gas_price_wei
    for operation in operations:
        validate_budget(state, operation, balance, gas, price)
        balance = balance.model_copy(update={"amount": balance.amount - gas * price})
    return balance


def entry_operations(config, operations):
    """Every operation up to and including the last one that adds exposure."""
    adding = [i for i, operation in enumerate(operations) if increases_exposure(config, operation)]
    return operations[: adding[-1] + 1]


def admitted_entry(state, policy, balance):
    """Admit every captured entry transaction, through the last that adds exposure."""
    operations = entry_operations(state.config, [o.operation for o in state.outcomes])
    return admitted_sequence(state, operations, policy, balance)


def test_entry_minimum_pays_for_every_captured_entry_transaction(production_debt):
    """The funding statement's minimum completes entry under fee admission's own rule."""
    gas = production_debt.config.production.gas[0]
    # At the fee ceiling and without a lifetime cap, only the payer balance can refuse.
    policy = gas.model_copy(
        update={
            "max_transaction_fee": gas.max_transaction_fee.model_copy(
                update={"amount": gas.max_gas_limit * gas.max_gas_price_wei}
            ),
            "total_budget": None,
        }
    )
    # Both accounts share one payer, so its minimum covers both accounts' entry.
    config = shared_operator_config(production_debt.config)
    config = config.model_copy(
        update={"production": config.production.model_copy(update={"gas": (policy,)})}
    )
    state = production_debt.model_copy(update={"config": config})
    accounts = payer_accounts(config, config.accounts.bindings[0].profile.submitter)
    assert set(accounts) == {b.name for b in config.accounts.bindings}
    entries = payer_entry_transactions(config, accounts)
    captured = entry_operations(config, [o.operation for o in state.outcomes])
    # Supply approvals and supplies, collateral enablement and the borrow.
    assert len(captured) > 1 and entries >= len(captured)
    funded = state.outcomes[0].before.native_balance.model_copy(
        update={"amount": native_entry_minimum(policy, config, accounts, entries)}
    )
    left = admitted_entry(state, policy, funded)
    assert left.amount >= policy.exit_reserve.amount
    # One fee plus the reserve, the former minimum, stops entry after its first transaction.
    short = funded.model_copy(update={"amount": admission_reserve(policy)})
    with pytest.raises(DataError, match="payer balance cannot cover"):
        admitted_entry(state, policy, short)


def test_entry_minimum_pays_for_approvals_that_add_no_exposure(monkeypatch):
    """A Uniswap V4 liquidity entry pays for the other currency's approvals before minting."""
    from almanak.connectors.uniswap_v4.execution import UniswapExecution
    from almanak.connectors.uniswap_v4.state import Approval, Mint, Swap
    from almanak.connectors.uniswap_v4.validation import increases_exposure as v4_exposure
    from almanak.gateway import production_fees

    funding, other = (
        AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        AssetType("eip155:1/erc20:0xc02aaa39a223e2b44c2edd18a0b6ec3fe9b69cfd"),
    )
    target = SimpleNamespace(account="fund", funding_currency=funding)

    def approval(kind, asset):
        return Approval.model_construct(target=target, kind=kind, asset=asset, amount=1)

    # The planner's order on a fresh fund: the entry swap's two approvals and the swap,
    # then both currencies' Permit2 and position manager approvals, then the mint.
    operations = [
        approval("approve_erc20", funding),
        approval("approve_permit2", funding),
        Swap.model_construct(target=target, amount=AssetAmount(asset=funding, amount=1)),
        approval("approve_erc20", funding),
        approval("approve_permit2", funding),
        approval("approve_erc20", other),
        approval("approve_permit2", other),
        Mint.model_construct(target=target),
    ]
    assert [v4_exposure(None, o) for o in operations] == [True] * 5 + [False] * 2 + [True]
    monkeypatch.setattr(production_fees, "increases_exposure", v4_exposure)
    policy = SimpleNamespace(
        max_gas_limit=100,
        max_gas_price_wei=10,
        gas_limit_buffer_bps=0,
        max_transaction_fee=SimpleNamespace(amount=1000),
        exit_reserve=SimpleNamespace(amount=5000),
        total_budget=None,
        asset="eip155:1/slip44:60",
    )
    payer = SimpleNamespace(profile=SimpleNamespace(submitter="payer"))
    config = SimpleNamespace(
        production=SimpleNamespace(policy=lambda account: policy),
        accounts=SimpleNamespace(binding=lambda account: payer),
        positions=(
            SimpleNamespace(
                account="fund",
                liquidity_enabled=True,
                capital=AssetAmount(asset=funding, amount=10**6),
                entry_swap_bps=10000,
            ),
        ),
    )
    state = SimpleNamespace(config=config, outcomes=(), pending=None)
    entries = UniswapExecution.entry_transactions(config, "fund")
    assert entries == len(operations)
    funded = AssetAmount(
        asset=AssetType(policy.asset),
        amount=native_entry_minimum(policy, config, ("fund",), entries),
    )
    assert admitted_sequence(state, operations, policy, funded).amount == 5000
    # Counting only the transactions that add exposure leaves the mint unfunded.
    short = funded.model_copy(update={"amount": native_entry_minimum(policy, config, ("fund",), 6)})
    with pytest.raises(DataError, match="payer balance cannot cover"):
        admitted_sequence(state, operations, policy, short)


def shared_operator_config(config):
    operator = config.accounts.bindings[0].profile.operator
    bindings = tuple(
        b.model_copy(update={"profile": b.profile.model_copy(update={"operator": operator})})
        for b in config.accounts.bindings
    )
    return config.model_copy(
        update={"accounts": config.accounts.model_copy(update={"bindings": bindings})}
    )


def test_shared_operator_costs_share_one_budget(production_debt):
    state = production_debt.model_copy(
        update={"config": shared_operator_config(production_debt.config)}
    )
    outcome = state.outcomes[0]
    own_cost = sum(
        o.gas_paid.amount
        for o in state.outcomes
        if o.operation.account == outcome.operation.account
    )
    policy = state.config.production.gas[0]
    capped = policy.model_copy(
        update={
            "total_budget": policy.total_budget.model_copy(update={"amount": own_cost + 100}),
            "exit_reserve": policy.exit_reserve.model_copy(update={"amount": 0}),
        }
    )
    config = state.config.model_copy(
        update={"production": state.config.production.model_copy(update={"gas": (capped,)})}
    )
    with pytest.raises(DataError, match="budget"):
        validate_budget(
            state.model_copy(update={"config": config}),
            outcome.operation,
            outcome.before.native_balance,
            10,
            10,
        )


def test_shared_operator_cannot_receive_conflicting_policies(production_debt):
    raw = shared_operator_config(production_debt.config).model_dump(mode="json")
    policy = raw["production"]["gas"][0]
    raw["production"]["gas"] = [{**policy, "accounts": [name]} for name in policy["accounts"]]
    with pytest.raises(ValueError, match="same gas policy"):
        LendingConfig.model_validate_json(json.dumps(raw))


def test_safe_credentials_match_operator_and_never_fund(production_debt):
    validate_markets(production_debt.config)
    profile = production_debt.config.accounts.bindings[0].profile
    signer = Signer.from_key(TEST_KEY)
    profile = profile.model_copy(
        update={"operator": profile.operator.model_copy(update={"address": signer.address.lower()})}
    )
    store = Signer.encrypt(TEST_KEY, "test", kdf="pbkdf2", iterations=1000)
    assert decrypt_signer(store, "test", profile).address == signer.address
    with pytest.raises(ValueError, match="submitter"):
        decrypt_signer(store, "test", profile.model_copy(update={"operator": profile.fund}))


def test_preflight_requires_weth_capital_without_a_usdc_interest_buffer(production_debt):
    from almanak.gateway.production_cli import fund_preflight

    config = production_debt.config
    binding = config.accounts.bindings[0]
    result = fund_preflight(config, binding, production_debt.baseline)
    tokens = {t["asset"]: t for t in result["tokens"]}
    debt = config.debts[0]
    # Interest is not prefunded: the borrowed asset carries no entry requirement.
    assert str(debt.reserve.asset) not in tokens
    assert len(tokens) == 1
    assert result["gas_payer"] == binding.profile.submitter.model_dump(mode="json")
