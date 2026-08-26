"""I3 PARAMETER FIDELITY on Base — compile-time, funds-free.

Every case declares an explicit non-zero ``max_slippage`` and asserts the
emitted calldata carries a bound the chain can enforce. Compilation is a
read-only fork operation: nothing is submitted and no funds move.

The predicate lives in ``tests/intents/_parameter_fidelity.py`` and was written
from the intent vocabulary and protocol ABIs alone. See
``docs/internal/qa-invariants/I3-parameter-fidelity.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.intents import LPOpenIntent, SwapIntent
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._parameter_fidelity import (
    RunOutcome,
    assert_parameter_fidelity,
    check_transactions,
    zero_constraints,
)

#: The intent-coverage gate reads function decorators, enclosing classes and
#: module-level ``pytestmark`` only. It cannot see the per-case markers
#: ``_PARAMS`` carries via ``pytest.param(marks=...)``, so without this the
#: module reads as unmarked. The per-case markers stay: they attribute each
#: case to its own primitive, which this module-level declaration cannot.
pytestmark = pytest.mark.intent(IntentType.SWAP, IntentType.LP_OPEN)

CHAIN_NAME = "base"

#: The tolerance every case declares. Any strictly positive value works — I3 is
#: about the bound being PRESENT and able to bind, not about its magnitude.
DECLARED_SLIPPAGE = Decimal("0.05")


@dataclass(frozen=True)
class Case:
    id: str
    protocol: str
    intent_kind: str
    kwargs: dict[str, Any]


_SWAP_CASES = [
    Case("uniswap_v3.swap", "uniswap_v3", "swap", {"from_token": "USDC", "to_token": "WETH", "amount": Decimal("100")}),
    Case(
        "sushiswap_v3.swap",
        "sushiswap_v3",
        "swap",
        {"from_token": "USDC", "to_token": "WETH", "amount": Decimal("100")},
    ),
    Case(
        "aerodrome.swap.cl", "aerodrome", "swap", {"from_token": "USDC", "to_token": "WETH", "amount": Decimal("100")}
    ),
    Case(
        "aerodrome.swap.classic",
        "aerodrome",
        "swap",
        {
            "from_token": "USDC",
            "to_token": "WETH",
            "amount": Decimal("100"),
            "swap_params": {"classic": True},
        },
    ),
    Case("curve.swap", "curve", "swap", {"from_token": "USDC", "to_token": "USDbC", "amount": Decimal("100")}),
]

_LP_OPEN_CASES = [
    Case(
        "uniswap_v3.lp_open",
        "uniswap_v3",
        "lp_open",
        {
            "pool": "WETH/USDC/3000",
            "amount0": Decimal("0.2"),
            "amount1": Decimal("500"),
            "range_lower": Decimal("200"),
            "range_upper": Decimal("20000"),
        },
    ),
    Case(
        "aerodrome.lp_open.classic",
        "aerodrome",
        "lp_open",
        {
            "pool": "USDC/WETH/volatile",
            "amount0": Decimal("10"),
            "amount1": Decimal("0.005"),
            "range_lower": Decimal("1"),
            "range_upper": Decimal("1000000"),
        },
    ),
    Case(
        "aerodrome_slipstream.lp_open",
        "aerodrome_slipstream",
        "lp_open",
        {
            "pool": "WETH/USDC/50",
            "amount0": Decimal("0.1"),
            "amount1": Decimal("250"),
            "range_lower": Decimal("-300000"),
            "range_upper": Decimal("200000"),
        },
    ),
    Case(
        "curve.lp_open",
        "curve",
        "lp_open",
        {
            "pool": "weth_cbeth",
            "amount0": Decimal("0.01"),
            "amount1": Decimal("0.01"),
            "range_lower": Decimal("1"),
            "range_upper": Decimal("1000000"),
        },
    ),
]


def _build(case: Case):
    if case.intent_kind == "swap":
        return SwapIntent(
            protocol=case.protocol,
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
            **case.kwargs,
        )
    return LPOpenIntent(
        protocol=case.protocol,
        chain=CHAIN_NAME,
        max_slippage=DECLARED_SLIPPAGE,
        **case.kwargs,
    )


_PARAMS = [pytest.param(case, id=case.id, marks=pytest.mark.intent(IntentType.SWAP)) for case in _SWAP_CASES] + [
    pytest.param(case, id=case.id, marks=pytest.mark.intent(IntentType.LP_OPEN)) for case in _LP_OPEN_CASES
]


@pytest.mark.parametrize("case", _PARAMS)
def test_declared_slippage_reaches_the_calldata(
    case: Case,
    anvil_rpc_url: str,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
) -> None:
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    result = compiler.compile(_build(case))

    if result.status is not CompilationStatus.SUCCESS:
        # A refusal is not an I3 violation — nothing was encoded. Report it so a
        # connector that silently stopped compiling is not mistaken for covered.
        pytest.skip(f"{case.id}: compilation did not succeed ({result.status.value}): {result.error}")

    report = check_transactions(
        result.transactions,
        label=f"{case.id} ({CHAIN_NAME})",
        declared_slippage=DECLARED_SLIPPAGE,
    )
    print("\n" + report.describe())
    assert_parameter_fidelity(report)


@pytest.mark.parametrize("case", _PARAMS)
def test_mutation_control_on_live_calldata(
    case: Case,
    anvil_rpc_url: str,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
) -> None:
    """MUTATION CONTROL: the same real calldata, with only its bounds zeroed.

    A green I3 on this connector means nothing unless the checker would have
    gone red had the floor been absent. This compiles the identical intent, takes
    the calldata the connector really produced, re-encodes it with every
    constraint parameter set to zero and nothing else changed, and requires the
    verdict to flip to VIOLATION.
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    result = compiler.compile(_build(case))
    if result.status is not CompilationStatus.SUCCESS:
        pytest.skip(f"{case.id}: compilation did not succeed ({result.status.value}): {result.error}")

    live = check_transactions(result.transactions, label=f"{case.id} live", declared_slippage=DECLARED_SLIPPAGE)
    if live.outcome is not RunOutcome.PASS:
        pytest.skip(f"{case.id}: live calldata is not PASS ({live.outcome.value}); mutation would prove nothing")

    mutated = check_transactions(
        [{"to": tx.to, "data": zero_constraints(tx.data)} for tx in result.transactions],
        label=f"{case.id} MUTATED (all bounds -> 0)",
        declared_slippage=DECLARED_SLIPPAGE,
    )
    print("\n" + mutated.describe())
    assert mutated.outcome is RunOutcome.VIOLATION, (
        f"{case.id}: zeroing every bound in the connector's own calldata did NOT flip the "
        f"verdict (got {mutated.outcome.value}). The green above is not evidence of detection."
    )
