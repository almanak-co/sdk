"""I3b — TOLERANCE RESPONSIVENESS: is the encoded bound DERIVED from the caller's tolerance?

I3 proper asks only whether a bound is present and able to bind. That is a real
property, and it is deliberately weaker than the thing a caller actually
believes when they set ``max_slippage``. A connector that ignores the field and
encodes a floor from its own hard-coded default passes I3 while silently
discarding the caller's declared risk parameter.

This probe closes that gap without any protocol-specific knowledge: compile the
SAME intent twice against the SAME fork state, once with a tight tolerance and
once with a wide one, and require the encoded ``min``-family bounds to MOVE. A
floor derived from the caller's tolerance must be strictly lower when the
tolerance is wider. A floor that is byte-identical across a 100x change in the
declared tolerance was not derived from it.

This is a diagnostic, not a safety gate: a bound that does not move is not by
itself a money-losing defect (a floor tighter than requested is fail-closed, and
merely wastes gas on revert). It is reported so that "the caller's parameter
reached the chain" is not confused with "a bound reached the chain".
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._parameter_fidelity import (
    ConstraintKind,
    RunOutcome,
    check_transactions,
)
from tests.intents.base.test_i3_parameter_fidelity import _PARAMS, CHAIN_NAME, Case, _build

#: Same reason as the sibling module: the intent-coverage gate cannot see the
#: per-case markers ``_PARAMS`` carries, and this module reuses that exact
#: parameter list, so it covers the same two primitives.
pytestmark = pytest.mark.intent(IntentType.SWAP, IntentType.LP_OPEN)

TIGHT = Decimal("0.001")
WIDE = Decimal("0.10")


def _floors(case: Case, slippage: Decimal, compiler: IntentCompiler) -> dict[str, int] | None:
    intent = _build(case)
    intent = intent.model_copy(update={"max_slippage": slippage})
    result = compiler.compile(intent)
    if result.status is not CompilationStatus.SUCCESS:
        return None
    report = check_transactions(result.transactions, label=f"{case.id}@{slippage}", declared_slippage=slippage)
    if report.outcome is not RunOutcome.PASS:
        return None
    out: dict[str, int] = {}
    for verdict in report.verdicts:
        stack = [verdict]
        while stack:
            node = stack.pop()
            stack.extend(node.sub_calls)
            for constraint in node.constraints:
                if constraint.kind is ConstraintKind.MIN and isinstance(constraint.value, int):
                    out[f"{node.function}:{constraint.path}"] = constraint.value
    return out


@pytest.mark.parametrize("case", _PARAMS)
def test_declared_tolerance_moves_the_encoded_floor(
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
    tight = _floors(case, TIGHT, compiler)
    wide = _floors(case, WIDE, compiler)
    if tight is None or wide is None:
        pytest.skip(f"{case.id}: could not compile a PROTECTED bundle at both tolerances")

    shared = sorted(set(tight) & set(wide))
    assert shared, f"{case.id}: no comparable min-family bound across the two compiles"

    print(f"\nI3b TOLERANCE RESPONSIVENESS — {case.id} ({CHAIN_NAME})")
    inert = []
    for key in shared:
        moved = "MOVED" if wide[key] < tight[key] else "INERT"
        if wide[key] >= tight[key]:
            inert.append(key)
        print(f"  {key}: tolerance {TIGHT} -> {tight[key]} | tolerance {WIDE} -> {wide[key]}  [{moved}]")

    assert not inert, (
        f"{case.id}: the encoded floor did NOT respond to a 100x change in the caller's "
        f"declared max_slippage ({TIGHT} -> {WIDE}) for {inert}. The bound is present, so I3 "
        f"passes, but it was not derived from the caller's tolerance."
    )
