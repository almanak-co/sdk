"""Retained Safe execution constrains final submission and settlement refusal paths."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.execution import AerodromeExecution
from almanak.connectors.aerodrome.receipts import movement
from almanak.connectors.aerodrome.reconciliation import allowances_after, validate_balances
from almanak.connectors.aerodrome.state import Checkpoint
from almanak.connectors.aerodrome.validation import validate_current
from almanak.gateway.ethereum import DataError
from tests.exchange_scope import without_exchange_budgets
from tests.test_aerodrome_native import CASES, EVIDENCE
from tests.test_aerodrome_native import candidate_binding as candidate_binding


@pytest.fixture
def state(candidate_binding):
    case = next(c for c in CASES if "volatile_lp-safe-configured" in c["name"])
    return Checkpoint.model_validate_json(
        json.dumps(
            without_exchange_budgets(
                json.loads(
                    gzip.decompress((EVIDENCE / case["name"] / "checkpoint.json.gz").read_bytes())
                )
            )
        )
    )


def effects(outcome):
    return movement(outcome.receipt, outcome.operation, outcome.before.pool.pool_fees)


def test_preflight_requires_one_correlated_account_and_unowned_initial_position(state):
    before = state.outcomes[0].before
    account = before.target.account
    for selected, observations in (("missing", (before,)), (account, (before, before))):
        with pytest.raises(ValueError, match="exactly"):
            AerodromeExecution.preflight(state.config, selected, observations)
    foreign = before.model_copy(
        update={
            "target": before.target.model_copy(update={"allocation": "foreign"}),
        }
    )
    with pytest.raises(ValueError, match="configured target"):
        AerodromeExecution.preflight(state.config, account, (foreign,))
    entered = next(o.after for o in state.outcomes if isinstance(o.operation, ops.Mint))
    with pytest.raises(DataError, match="initial fund"):
        AerodromeExecution.preflight(state.config, account, (entered,))
    tokens, _ = AerodromeExecution.preflight(state.config, account, (before,))
    assert tokens[0]["funded"]
    allocations = state.config.allocations
    allocation = next(a for a in allocations if a.account == account)
    capital = allocation.capital.model_copy(update={"amount": allocation.capital.amount * 2})
    widened = allocation.model_copy(update={"capital": capital})
    doubled = state.config.model_copy(
        update={"allocations": tuple(widened if a is allocation else a for a in allocations)}
    )
    tokens, _ = AerodromeExecution.preflight(doubled, account, (before,))
    assert tokens[0]["required_token_base_units"] == str(capital.amount)
    assert not tokens[0]["funded"]


def test_submission_refuses_simulation_changed_allowance_and_expired_protocol_deadline(state):
    outcome = next(o for o in state.outcomes if isinstance(o.operation, ops.Swap))
    backend = SimpleNamespace(config=state.config)
    with pytest.raises(ValueError, match="simulated"):
        validate_current(backend, outcome, outcome.before.model_copy(update={"simulated": True}))
    allowance = outcome.before.allowances[0]
    changed = outcome.before.model_copy(
        update={
            "allowances": (allowance.model_copy(update={"amount": allowance.amount + 1}),)
            + outcome.before.allowances[1:],
        }
    )
    with pytest.raises(DataError, match="spending authority"):
        validate_current(backend, outcome, changed)
    expired = outcome.model_copy(
        update={
            "operation": outcome.operation.model_copy(
                update={"deadline": outcome.before.block.timestamp - 1}
            ),
        }
    )
    with pytest.raises(DataError, match="protocol deadline"):
        validate_current(backend, expired, outcome.before)


def test_a_signed_approval_is_rebroadcast_after_its_intent_expires(state):
    outcome = next(o for o in state.outcomes if isinstance(o.operation, ops.Approval))
    backend = SimpleNamespace(config=state.config)
    intent = outcome.decision.intent
    pool = outcome.before.pool
    block = pool.block.model_copy(update={"timestamp": intent.deadline})
    expired = outcome.before.model_copy(update={"pool": pool.model_copy(update={"block": block})})
    unsigned = SimpleNamespace(
        before=outcome.before, operation=outcome.operation, decision=outcome.decision
    )
    with pytest.raises(DataError, match="retained deadline expired"):
        validate_current(backend, unsigned, expired)
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), expired)


def test_submission_refuses_foreign_lp_deposit_before_a_pool_burn(state):
    outcome = next(o for o in state.outcomes if isinstance(o.operation, ops.Removal))
    current = outcome.before.model_copy(
        update={
            "pool": outcome.before.pool.model_copy(update={"deposited_shares": 1}),
        }
    )
    with pytest.raises(DataError, match="another transfer"):
        validate_current(SimpleNamespace(config=state.config), outcome, current)


def test_failed_receipts_preserve_authority_and_finite_allowances_cannot_be_overdrawn(state):
    outcome = next(o for o in state.outcomes if isinstance(o.operation, ops.Swap))
    before, operation, effect = outcome.before, outcome.operation, effects(outcome)
    assert allowances_after(before, operation, effect, False) == before.allowances
    for amount in (0, 2**256 - 1):
        changed = before.model_copy(
            update={
                "allowances": tuple(
                    a.model_copy(update={"amount": amount})
                    if a.asset == operation.amount.asset
                    else a
                    for a in before.allowances
                ),
            }
        )
        if amount == 0:
            with pytest.raises(DataError, match="exceeds its retained allowance"):
                allowances_after(changed, operation, effect, True)
        else:
            assert allowances_after(changed, operation, effect, True) == changed.allowances


def test_settlement_refuses_changed_authority_and_payer_but_retains_external_cash_issues(state):
    outcome = next(o for o in state.outcomes if isinstance(o.operation, ops.Mint))
    after, effect, gas = outcome.after, effects(outcome), outcome.gas_paid.amount
    for field, value, message in (
        ("position", None, "LP checkpoint"),
        ("allowances", outcome.before.allowances, "allowance differs"),
        ("payer", after.payer.model_copy(update={"address": "0x" + "ab" * 20}), "payer scope"),
    ):
        with pytest.raises(DataError, match=message):
            validate_balances(outcome, after.model_copy(update={field: value}), effect, gas, True)
    changed = after.model_copy(
        update={
            "wallet0": after.wallet0.model_copy(update={"amount": after.wallet0.amount + 1}),
            "native_balance": after.native_balance.model_copy(
                update={"amount": after.native_balance.amount + 1}
            ),
        }
    )
    issues = validate_balances(outcome, changed, effect, gas, True)
    assert len(issues) == 2 and all("unexplained external movements" in issue for issue in issues)
