"""Foundational QA matrix: semantic mutations and production-shape fidelity.

Ticket regressions are examples, not the organizing principle.  Each row names
one law that applies across connectors and strategies.  Known product gaps are
strict xfails: the target assertion still runs, and a fix produces XPASS so the
row must be deliberately promoted to green.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.cli._run_modes import _lifecycle_coverage
from almanak.framework.dashboard.templates.lp_dashboard import (
    LPDashboardConfig,
    _hydrate_active_position_from_events,
)
from almanak.framework.execution.orchestrator import ExecutionResult as OrchestratorExecutionResult
from almanak.framework.intents import Intent
from almanak.framework.runner.runner_models import IterationStatus
from almanak.framework.teardown import PositionInfo, PositionType, generate_lending_unwind
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.slippage_manager import (
    ExecutionAttempt as SlippageExecutionAttempt,
)
from almanak.framework.teardown.slippage_manager import (
    ExecutionResult as SlippageExecutionResult,
)
from almanak.framework.teardown.token_post_condition import token_balance_teardown_post_condition
from almanak.framework.testing.semantic_invariants import (
    InvariantFamily,
    InvariantSpec,
    Mutation,
    equivalent_observation_diffs,
    identity_operations,
    missing_evidence_fields,
    richer_than_production_fields,
    validate_invariant_catalog,
)

_AS_OF = "2026-08-13"
_USDC_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


INVARIANT_CATALOG = (
    InvariantSpec(
        invariant_id="identity.representation-equivalence",
        family=InvariantFamily.IDENTITY,
        positive_controls=("symbol-spelled close covers the symbol-spelled position",),
        mutations=("symbol to exact chain address", "case mutation", "cross-chain discriminator"),
        production_surface="teardown.completeness.check_intent_coverage",
    ),
    InvariantSpec(
        invariant_id="identity.owned-delta-not-wallet-total",
        family=InvariantFamily.IDENTITY,
        positive_controls=("zero terminal strategy-owned token quantity certifies closure",),
        mutations=("unrelated pre-existing wallet balance",),
        production_surface="teardown.token_post_condition.token_balance_teardown_post_condition",
    ),
    InvariantSpec(
        invariant_id="teardown_algebra.no-identity-operation",
        family=InvariantFamily.TEARDOWN_ALGEBRA,
        positive_controls=("cross-asset unwind retains the required swap",),
        mutations=("collateral and debt resolve to the same asset",),
        production_surface="teardown.generate_lending_unwind",
    ),
    InvariantSpec(
        invariant_id="boundary_fidelity.execution-evidence-survives-wrappers",
        family=InvariantFamily.BOUNDARY_FIDELITY,
        positive_controls=("authoritative orchestrator result declares transaction_results",),
        mutations=("successful slippage wrapper", "failed slippage attempt"),
        production_surface="orchestrator -> escalating slippage -> teardown manager",
    ),
    InvariantSpec(
        invariant_id="projection_parity.canonical-lp-token-order",
        family=InvariantFamily.PROJECTION_PARITY,
        positive_controls=("canonical event order projects canonical token amounts",),
        mutations=("presentation config reverses token order",),
        production_surface="dashboard.lp_dashboard._hydrate_active_position_from_events",
    ),
    InvariantSpec(
        invariant_id="lifecycle_coverage.requested-path-is-not-safe-completion",
        family=InvariantFamily.LIFECYCLE_COVERAGE,
        positive_controls=("receipt-backed intent is exercised",),
        mutations=("requested action returns HOLD", "success has no effective evidence"),
        production_surface="cli._run_modes._lifecycle_coverage",
    ),
)


def test_invariant_catalog_has_scientific_controls_for_every_family() -> None:
    assert validate_invariant_catalog(INVARIANT_CATALOG) == ()
    assert {spec.family for spec in INVARIANT_CATALOG} == set(InvariantFamily)


def test_equivalence_harness_reports_the_mutation_not_the_control() -> None:
    diffs = equivalent_observation_diffs(
        "USDC",
        (Mutation("case", "usdc"), Mutation("different asset", "WETH")),
        str.upper,
    )
    assert [(diff.mutation, diff.control, diff.observed) for diff in diffs] == [("different asset", "USDC", "WETH")]


def _token_position(*, chain: str = "ethereum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="wallet-usdc",
        chain=chain,
        protocol="wallet",
        value_usd=Decimal("10"),
        details={"asset": "USDC"},
    )


def _swap_coverage(from_token: str, *, chain: str = "ethereum") -> bool:
    intent = Intent.swap(from_token=from_token, to_token="WETH", amount="all", chain=chain)
    return check_intent_coverage([_token_position()], [intent]).complete


def test_identity_control_and_cross_chain_discriminator_are_not_conflated() -> None:
    assert _swap_coverage("USDC") is True
    assert _swap_coverage("USDC", chain="base") is False


def test_equivalent_token_representations_preserve_teardown_coverage() -> None:
    """PROMOTED from strict xfail (ALM-3105) on 2026-08-21.

    The gap was "teardown coverage has no chain-aware symbol/address identity
    resolver". `2a6e404a9c fix(teardown): reconcile completeness by asset
    identity (#3778)` supplied one, and the row began to XPASS, which is the
    signal this matrix exists to produce.

    Promoted rather than absorbed, and only after checking the pass is not
    vacuous: `diffs == ()` asserts the three representations AGREE, which an
    all-False resolver would also satisfy. Measured directly — "USDC", the
    exact address, and "usdc" each return coverage=True, while the
    cross-chain control on `base` still returns False. So the row is green
    because the resolver works, not because it stopped discriminating; the
    sibling test above pins that control independently.
    """
    diffs = equivalent_observation_diffs(
        "USDC",
        (Mutation("exact address", _USDC_ETHEREUM), Mutation("case", "usdc")),
        _swap_coverage,
    )
    assert diffs == ()


class _TokenGateway:
    def __init__(self, balance: int) -> None:
        self.balance = balance

    def query_erc20_balance(self, **_kwargs: Any) -> int:
        return self.balance


def _token_closure(wallet_balance: int) -> bool:
    result = token_balance_teardown_post_condition(
        SimpleNamespace(
            protocol="wallet",
            position_id="strategy-owned-usdc",
            chain="ethereum",
            details={"token_address": _USDC_ETHEREUM},
        ),
        "0x1111111111111111111111111111111111111111",
        gateway_client=_TokenGateway(wallet_balance),
        block=123,
    )
    return result.closed


def test_zero_terminal_wallet_balance_is_a_measured_token_closure() -> None:
    assert _token_closure(0) is True


@pytest.mark.xfail(
    strict=True,
    reason=(
        f"ALM-3102/ALM-3298 as of {_AS_OF}: token closure compares whole-wallet balance "
        "instead of the strategy-owned pre/post delta"
    ),
)
def test_unrelated_wallet_inventory_does_not_change_strategy_closure() -> None:
    diffs = equivalent_observation_diffs(
        0,
        (Mutation("pre-existing wallet inventory", 1_000_000),),
        _token_closure,
    )
    assert diffs == ()


class _Health:
    collateral_value_usd = Decimal("100")
    debt_value_usd = Decimal("40")
    lltv = Decimal("0.8")


class _Balance:
    balance = Decimal("0")


class _LendingMarket:
    chain = "ethereum"

    def price(self, _token: str) -> Decimal:
        return Decimal("1")

    def balance(self, _token: str, chain: str | None = None) -> _Balance:  # noqa: ARG002
        return _Balance()

    def position_health(self, **_kwargs: Any) -> _Health:
        return _Health()


def _unwind(collateral: str, debt: str) -> list[Any]:
    return generate_lending_unwind(
        market=_LendingMarket(),
        protocol="aave_v3",
        collateral_token=collateral,
        borrow_token=debt,
        chain="ethereum",
    )


def _same_domain_swaps(operations: list[Any]) -> tuple[object, ...]:
    return identity_operations(
        operations,
        source=lambda operation: getattr(operation, "from_token", None),
        target=lambda operation: getattr(operation, "to_token", None),
        domain=lambda operation: getattr(operation, "chain", None),
    )


def test_cross_asset_unwind_keeps_economically_required_swaps() -> None:
    operations = _unwind("WETH", "USDC")
    assert any(type(operation).__name__ == "SwapIntent" for operation in operations)
    assert _same_domain_swaps(operations) == ()


@pytest.mark.xfail(
    strict=True,
    reason=f"ALM-3036 as of {_AS_OF}: same-asset lending unwind emits collateral-to-itself swaps",
)
def test_same_asset_unwind_contains_no_identity_operation() -> None:
    assert _same_domain_swaps(_unwind("USDC", "USDC")) == ()


def test_test_double_cannot_hide_a_production_boundary_evidence_loss() -> None:
    fake = SimpleNamespace(
        success=True,
        final_slippage=Decimal("0.005"),
        transaction_results=[SimpleNamespace(receipt=SimpleNamespace(block_number=123))],
    )
    assert "transaction_results" in richer_than_production_fields(fake, SlippageExecutionResult)


def test_authoritative_orchestrator_result_declares_transaction_evidence() -> None:
    assert missing_evidence_fields(OrchestratorExecutionResult, ("transaction_results",)) == ()


@pytest.mark.xfail(
    strict=True,
    reason=(
        f"ALM-3267/ALM-3277 as of {_AS_OF}: slippage result types discard receipt evidence "
        "before teardown reconciliation and block-pinned verification"
    ),
)
def test_receipt_evidence_survives_success_and_failure_wrappers() -> None:
    assert missing_evidence_fields(SlippageExecutionAttempt, ("transaction_results",)) == ()
    assert missing_evidence_fields(SlippageExecutionResult, ("transaction_results",)) == ()


def _open_event() -> list[dict[str, Any]]:
    return [
        {
            "position_id": "42",
            "event_type": "OPEN",
            "timestamp": "2026-08-13T00:00:00Z",
            "amount0": "1000000000000000",
            "amount1": "2000000",
            "tick_lower": -201930,
            "tick_upper": -199920,
            "value_usd": "4",
        }
    ]


def _lp_projection(token_order: tuple[str, str]) -> dict[str, float]:
    config = LPDashboardConfig(token0=token_order[0], token1=token_order[1], chain="arbitrum")
    result: dict[str, Any] = {}
    _hydrate_active_position_from_events(result, _open_event(), config)
    return {
        config.token0: result["token0_amount"],
        config.token1: result["token1_amount"],
    }


def test_canonical_lp_event_order_has_expected_economic_projection() -> None:
    assert _lp_projection(("WETH", "USDC")) == {"WETH": 0.001, "USDC": 2.0}


@pytest.mark.xfail(
    strict=True,
    reason=f"ALM-3059 as of {_AS_OF}: LP dashboard projects canonical event slots through config order",
)
def test_lp_projection_is_invariant_to_presentation_token_order() -> None:
    diffs = equivalent_observation_diffs(
        ("WETH", "USDC"),
        (Mutation("reversed presentation order", ("USDC", "WETH")),),
        _lp_projection,
    )
    assert diffs == ()


def test_requested_lifecycle_path_requires_effective_evidence() -> None:
    executed = {
        "status": IterationStatus.SUCCESS.value,
        "intent": {"type": "SWAP"},
        "execution_result": {"success": True, "tx_hashes": ["0xabc"]},
    }
    held = {"status": IterationStatus.HOLD.value}
    unmeasured = {
        "status": IterationStatus.SUCCESS.value,
        "intent": {"type": "SWAP"},
        "execution_result": {"success": True, "tx_hashes": []},
    }

    assert (
        _lifecycle_coverage([executed], None, requested_actions=["open"], teardown_requested=False)[
            "requested_paths_exercised"
        ]
        is True
    )
    assert (
        _lifecycle_coverage([held], None, requested_actions=["open"], teardown_requested=False)[
            "requested_paths_exercised"
        ]
        is False
    )
    assert (
        _lifecycle_coverage([unmeasured], None, requested_actions=["open"], teardown_requested=False)[
            "requested_paths_exercised"
        ]
        is False
    )
