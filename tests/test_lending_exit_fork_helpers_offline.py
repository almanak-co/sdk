"""Fork-proof helpers that check lending exits run offline on constructed inputs."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.fork.production_setup import loan_asset_funding, rehearsal_config

ASSERTIONS = Path(__file__).parent / "strategy_fork/scenarios/aave_v4_weth_supply/assertions.py"


def reference_assertions():
    spec = importlib.util.spec_from_file_location("aave_v4_weth_supply_assertions", ASSERTIONS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def outcome(allocation, kind, assets):
    return SimpleNamespace(
        operation=SimpleNamespace(allocation=allocation, kind=kind), assets=assets
    )


def state(*outcomes):
    return SimpleNamespace(outcomes=outcomes)


DEBT = SimpleNamespace(name="main_usdc")


def test_one_full_repay_above_the_borrowed_amount_has_no_upper_bound():
    check = reference_assertions().assert_repayment_cost
    unrelated = outcome("main_usdc_1", "repay", 1)
    check(
        state(
            outcome("main_usdc", "borrow", 5_000_000),
            outcome("main_usdc", "repay", 5_000_001),
            unrelated,
        ),
        DEBT,
    )
    check(
        state(outcome("main_usdc", "borrow", 5_000_000), outcome("main_usdc", "repay", 50_000_000)),
        DEBT,
    )


@pytest.mark.parametrize(
    "outcomes",
    [
        # Repaying no more than was borrowed means no interest was paid.
        (("borrow", 5_000_000), ("repay", 5_000_000)),
        # A full repay is one transaction, not a sequence of partial ones.
        (("borrow", 5_000_000), ("repay", 3_000_000), ("repay", 2_000_001)),
        (("borrow", 4_000_000), ("repay", 4_000_001)),
        (("borrow", 2_500_000), ("borrow", 2_500_000), ("repay", 5_000_001)),
        (("borrow", 5_000_000),),
    ],
)
def test_repayment_cost_refuses_missing_interest_split_repays_and_extra_borrows(outcomes):
    check = reference_assertions().assert_repayment_cost
    with pytest.raises(AssertionError):
        check(state(*(outcome("main_usdc", kind, assets) for kind, assets in outcomes)), DEBT)


def test_eoa_rehearsal_funds_each_debt_with_the_safe_fixture_loan_balance():
    config = rehearsal_config("safe_borrow")
    assert {debt.name for debt in config.debts} == {"main_usdc", "main_usdc_1"}
    for debt in config.debts:
        # The fixture's loan balance pays accrued interest; it is not tied to the debt amount.
        assert loan_asset_funding(debt) == 1_000_000
        assert "amount" not in type(debt).model_fields
    with pytest.raises(StopIteration):
        loan_asset_funding(SimpleNamespace(name="missing", reserve=config.debts[0].reserve))
