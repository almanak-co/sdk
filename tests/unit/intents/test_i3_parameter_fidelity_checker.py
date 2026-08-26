"""Liveness tests for the I3 PARAMETER FIDELITY checker.

A check that has only ever passed has never demonstrated detection. Every
``PROTECTED`` case below is paired with a MUTATION CONTROL: the same real
selector, the same real ABI, one constraint parameter forced to its
non-binding sentinel — and the checker must report ``VIOLATION``.

The calldata here is encoded with ``eth_abi`` against the same ABI fragments the
checker resolves, so these are real selectors and real ABI encodings rather than
hand-written strings that could drift from what a chain would accept.

See ``docs/internal/qa-invariants/I3-parameter-fidelity.md``.
"""

from __future__ import annotations

import pytest
from eth_abi import encode as abi_encode
from eth_utils import function_signature_to_4byte_selector

from tests.intents._parameter_fidelity import (
    ConstraintKind,
    FidelityReport,
    RunOutcome,
    TxOutcome,
    assert_parameter_fidelity,
    check_calldata,
    check_transactions,
    classify_param,
    registry,
)

WALLET = "0x1111111111111111111111111111111111111111"
TOKEN_A = "0x2222222222222222222222222222222222222222"
TOKEN_B = "0x3333333333333333333333333333333333333333"
ROUTER = "0x4444444444444444444444444444444444444444"

UINT128_MAX = (1 << 128) - 1


def _encode(signature: str, types: list[str], values: list) -> str:
    return "0x" + (function_signature_to_4byte_selector(signature) + abi_encode(types, values)).hex()


# ---------------------------------------------------------------------------
# The name classifier — the whole predicate rests on it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("amountOutMinimum", ConstraintKind.MIN),
        ("amountOutMin", ConstraintKind.MIN),
        ("amountAMin", ConstraintKind.MIN),  # acronym boundary: amount|A|Min
        ("amountXMin", ConstraintKind.MIN),
        ("amount0Min", ConstraintKind.MIN),
        ("min_dy", ConstraintKind.MIN),
        ("min_mint_amount", ConstraintKind.MIN),
        ("min_amounts", ConstraintKind.MIN),
        ("amountInMaximum", ConstraintKind.MAX),
        ("amount0Max", ConstraintKind.MAX),
        ("max_burn_amount", ConstraintKind.MAX),
        ("sqrtPriceLimitX96", ConstraintKind.LIMIT),
        # Not constraints — these must not be swept in, or a PASS becomes free.
        ("deadline", None),
        ("amount0Desired", None),
        ("tickLower", None),
        ("liquidity", None),
        ("recipient", None),
        ("idSlippage", None),
    ],
)
def test_classify_param(name: str, expected: ConstraintKind | None) -> None:
    assert classify_param(name) is expected


# ---------------------------------------------------------------------------
# Uniswap V3 SwapRouter — exactInputSingle
# ---------------------------------------------------------------------------

_EXACT_INPUT_SINGLE = "exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
_EXACT_INPUT_SINGLE_TYPES = ["(address,address,uint24,address,uint256,uint256,uint256,uint160)"]


def _exact_input_single(amount_out_minimum: int) -> str:
    return _encode(
        _EXACT_INPUT_SINGLE,
        _EXACT_INPUT_SINGLE_TYPES,
        [(TOKEN_A, TOKEN_B, 500, WALLET, 1_800_000_000, 10**18, amount_out_minimum, 0)],
    )


def test_v3_swap_with_floor_is_protected() -> None:
    verdict = check_calldata(ROUTER, _exact_input_single(1_990_000))
    assert verdict.outcome is TxOutcome.PROTECTED
    floors = {c.path: c for c in verdict.constraints}
    assert floors["params.amountOutMinimum"].effective is True
    # The zero sqrtPriceLimitX96 sentinel is correctly read as non-binding.
    assert floors["params.sqrtPriceLimitX96"].effective is False


def test_v3_swap_with_zero_floor_is_a_violation() -> None:
    """MUTATION CONTROL: the only change is amountOutMinimum 1_990_000 -> 0."""
    verdict = check_calldata(ROUTER, _exact_input_single(0))
    assert verdict.outcome is TxOutcome.UNPROTECTED
    assert all(not c.effective for c in verdict.constraints)


# ---------------------------------------------------------------------------
# Solidly / Aerodrome classic router — the ALM-3367 shape
# ---------------------------------------------------------------------------

_ADD_LIQUIDITY = "addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)"
_ADD_LIQUIDITY_TYPES = ["address", "address", "bool", "uint256", "uint256", "uint256", "uint256", "address", "uint256"]


def _classic_add_liquidity(amount_a_min: int, amount_b_min: int) -> str:
    return _encode(
        _ADD_LIQUIDITY,
        _ADD_LIQUIDITY_TYPES,
        [TOKEN_A, TOKEN_B, False, 10**18, 2000 * 10**6, amount_a_min, amount_b_min, WALLET, 1_800_000_000],
    )


def test_classic_lp_mint_with_floors_is_protected() -> None:
    verdict = check_calldata(ROUTER, _classic_add_liquidity(99 * 10**16, 1980 * 10**6))
    assert verdict.outcome is TxOutcome.PROTECTED


def test_classic_lp_mint_with_zero_floors_is_a_violation() -> None:
    """MUTATION CONTROL: this is exactly the ALM-3367 calldata shape."""
    verdict = check_calldata(ROUTER, _classic_add_liquidity(0, 0))
    assert verdict.outcome is TxOutcome.UNPROTECTED
    assert {c.path for c in verdict.constraints} == {"amountAMin", "amountBMin"}


def test_one_zero_leg_is_still_protected() -> None:
    """A single zero leg is SUPPORTED by the intent contract for a one-sided LP.

    ``LPOpenIntent.require_two_sided_minimums`` defaults to false, so the checker
    must not call this a violation. The zero leg is still recorded.
    """
    verdict = check_calldata(ROUTER, _classic_add_liquidity(99 * 10**16, 0))
    assert verdict.outcome is TxOutcome.PROTECTED
    assert [c.effective for c in verdict.constraints] == [True, False]


# ---------------------------------------------------------------------------
# Curve — vector minimums
# ---------------------------------------------------------------------------


def test_curve_add_liquidity_floor() -> None:
    protected = check_calldata(
        ROUTER,
        _encode("add_liquidity(uint256[2],uint256)", ["uint256[2]", "uint256"], [[10**18, 10**18], 19 * 10**17]),
    )
    assert protected.outcome is TxOutcome.PROTECTED

    # MUTATION CONTROL: min_mint_amount -> 0
    unprotected = check_calldata(
        ROUTER,
        _encode("add_liquidity(uint256[2],uint256)", ["uint256[2]", "uint256"], [[10**18, 10**18], 0]),
    )
    assert unprotected.outcome is TxOutcome.UNPROTECTED


def test_curve_remove_liquidity_vector_floor() -> None:
    protected = check_calldata(
        ROUTER,
        _encode(
            "remove_liquidity(uint256,uint256[2])", ["uint256", "uint256[2]"], [10**18, [49 * 10**16, 49 * 10**16]]
        ),
    )
    assert protected.outcome is TxOutcome.PROTECTED

    # MUTATION CONTROL: the whole min_amounts vector -> zero
    unprotected = check_calldata(
        ROUTER,
        _encode("remove_liquidity(uint256,uint256[2])", ["uint256", "uint256[2]"], [10**18, [0, 0]]),
    )
    assert unprotected.outcome is TxOutcome.UNPROTECTED


# ---------------------------------------------------------------------------
# Sentinels: a nominal bound that cannot bind is not a bound
# ---------------------------------------------------------------------------


def test_uint128_max_cap_is_not_an_effective_bound() -> None:
    """``collect`` with amount*Max at the type maximum is 'no cap', not a cap."""
    verdict = check_calldata(
        ROUTER,
        _encode(
            "collect((uint256,address,uint128,uint128))",
            ["(uint256,address,uint128,uint128)"],
            [(42, WALLET, UINT128_MAX, UINT128_MAX)],
        ),
    )
    assert verdict.outcome is TxOutcome.UNPROTECTED
    assert all(not c.effective for c in verdict.constraints)


# ---------------------------------------------------------------------------
# multicall: the bound lives in a sub-call
# ---------------------------------------------------------------------------


def _multicall(payloads: list[str]) -> str:
    return _encode("multicall(bytes[])", ["bytes[]"], [[bytes.fromhex(p[2:]) for p in payloads]])


_DECREASE = "decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))"
_DECREASE_TYPES = ["(uint256,uint128,uint256,uint256,uint256)"]


def _decrease_liquidity(amount0_min: int, amount1_min: int) -> str:
    return _encode(_DECREASE, _DECREASE_TYPES, [(42, 10**12, amount0_min, amount1_min, 1_800_000_000)])


def test_multicall_sees_through_to_the_subcall_floor() -> None:
    verdict = check_calldata(ROUTER, _multicall([_decrease_liquidity(10**17, 200 * 10**6)]))
    assert verdict.outcome is TxOutcome.PROTECTED
    assert verdict.sub_calls[0].outcome is TxOutcome.PROTECTED


def test_multicall_with_zero_subcall_floors_is_a_violation() -> None:
    """MUTATION CONTROL: a V3-shaped close whose minimums are literal zero."""
    verdict = check_calldata(ROUTER, _multicall([_decrease_liquidity(0, 0)]))
    assert verdict.outcome is TxOutcome.UNPROTECTED
    assert verdict.sub_calls[0].outcome is TxOutcome.UNPROTECTED


# ---------------------------------------------------------------------------
# Fail-closed behaviour
# ---------------------------------------------------------------------------


def test_unknown_selector_is_inconclusive_never_a_pass() -> None:
    report = check_transactions(
        [{"to": ROUTER, "data": "0xdeadbeef" + "00" * 32}],
        label="unknown",
        declared_slippage="0.005",
    )
    assert report.verdicts[0].outcome is TxOutcome.UNKNOWN_SELECTOR
    assert report.outcome is RunOutcome.INCONCLUSIVE
    with pytest.raises(AssertionError, match="I3 PARAMETER FIDELITY"):
        assert_parameter_fidelity(report)


def test_approve_only_bundle_is_inconclusive_not_a_pass() -> None:
    """An approve carries no bound; a bundle of only approvals proves nothing."""
    approve = _encode("approve(address,uint256)", ["address", "uint256"], [ROUTER, 10**18])
    report = check_transactions([{"to": TOKEN_A, "data": approve}], label="approve-only", declared_slippage="0.005")
    assert report.verdicts[0].outcome is TxOutcome.NOT_MONEY_PATH
    assert report.outcome is RunOutcome.INCONCLUSIVE


def test_approve_then_protected_swap_passes() -> None:
    approve = _encode("approve(address,uint256)", ["address", "uint256"], [ROUTER, 10**18])
    report = check_transactions(
        [{"to": TOKEN_A, "data": approve}, {"to": ROUTER, "data": _exact_input_single(1_990_000)}],
        label="swap",
        declared_slippage="0.005",
    )
    assert report.outcome is RunOutcome.PASS
    assert_parameter_fidelity(report)


def test_a_violation_anywhere_in_the_bundle_fails_the_run() -> None:
    """MUTATION CONTROL at the bundle level."""
    approve = _encode("approve(address,uint256)", ["address", "uint256"], [ROUTER, 10**18])
    report = check_transactions(
        [{"to": TOKEN_A, "data": approve}, {"to": ROUTER, "data": _exact_input_single(0)}],
        label="swap",
        declared_slippage="0.005",
    )
    assert report.outcome is RunOutcome.VIOLATION
    with pytest.raises(AssertionError):
        assert_parameter_fidelity(report)


def test_empty_bundle_is_inconclusive() -> None:
    assert FidelityReport(label="empty", declared_slippage="0.005").outcome is RunOutcome.INCONCLUSIVE


def test_registry_resolves_the_vendored_protocol_artifacts() -> None:
    """The in-repo ABI JSON artifacts must actually load, or coverage is fiction."""
    reg = registry()
    assert len(reg) > 200
    # TraderJoe V2's LBRouter is vendored as a JSON artifact; its swap entrypoint
    # must resolve, otherwise a traderjoe run would silently read UNKNOWN.
    selector = function_signature_to_4byte_selector(
        "swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)"
    )
    fn = reg.lookup(selector)
    assert fn is not None and fn.name == "swapExactTokensForTokens"
