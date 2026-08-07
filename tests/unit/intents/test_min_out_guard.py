"""Tests for the protective-minimum guard and the shared `[0, 1)` slippage bound.

Two things are under test here, and they have very different reachability today
(VIB-6217 / VIB-6218):

``validate_max_slippage_fraction``
    IMMEDIATELY REACHABLE. It is wired into all seven intent validators, so every
    assertion in ``TestIntentValidatorsRejectFullSlippage`` exercises a real
    production entry point: constructing the intent is what a strategy does.

``derive_min_out`` / ``require_protective_min`` / ``slippage_bps_to_fraction``
    NOT YET REACHABLE from any production path. They are the shared contract that
    VIB-6219 (GMX) and VIB-6220 (Uniswap V3) wire into their compilers. Until those
    land, these tests prove the guard is *correct*, NOT that anything is *guarded*.
    This distinction is deliberate and load-bearing: this repo has a history of
    unit-tested safety machinery that never reached the executing path (nine
    connectors with a dead ``default_slippage_bps``; Pendle's ``estimate_output``
    with zero production callers). Do not read this file as coverage of the
    encode boundary.

Every bound assertion below checks the ERROR MESSAGE, not merely that *something*
raised. Several of these intents have unrelated validators that also reject
(``PerpOpenIntent`` requires ``leverage >= 1.1`` on gmx_v2, for instance), so a
bare ``pytest.raises(ValidationError)`` would go green whether or not the slippage
bound exists at all.
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents.bridge import BridgeIntent, InvalidBridgeError
from almanak.framework.intents.ensure_balance import (
    EnsureBalanceIntent,
    InvalidEnsureBalanceError,
)
from almanak.framework.intents.min_out_guard import (
    MAX_SLIPPAGE_BPS_EXCLUSIVE,
    UnprotectedTradeError,
    derive_min_out,
    require_protective_min,
    slippage_bps_to_fraction,
    validate_max_slippage_fraction,
)
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent

pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")

#: The message fragment every rejection must carry. Asserting on this is what
#: keeps a test from passing because some unrelated validator fired first.
BOUND_MSG = "must be in [0, 1)"


# ---------------------------------------------------------------------------
# validate_max_slippage_fraction — the shared bound (VIB-6217)
# ---------------------------------------------------------------------------


class TestValidateMaxSlippageFraction:
    """The single definition of which slippage tolerances are legal."""

    def test_rejects_exactly_one(self):
        """1 is the motivating defect: it derives a zero minimum from any amount."""
        with pytest.raises(ValueError, match=r"must be in \[0, 1\)"):
            validate_max_slippage_fraction(Decimal("1"))

    def test_accepts_the_value_just_below_one(self):
        """The bound is exclusive at 1 only — it must not clamp the whole top end."""
        assert validate_max_slippage_fraction(Decimal("0.9999")) is None

    def test_accepts_zero(self):
        """0 means "no tolerance", which is the strictest floor, not an absent one."""
        assert validate_max_slippage_fraction(Decimal("0")) is None

    def test_rejects_above_one(self):
        with pytest.raises(ValueError, match=r"must be in \[0, 1\)"):
            validate_max_slippage_fraction(Decimal("1.5"))

    def test_rejects_negative(self):
        with pytest.raises(ValueError, match=r"must be in \[0, 1\)"):
            validate_max_slippage_fraction(Decimal("-0.01"))

    def test_none_is_skipped(self):
        """The optional fields (LPOpen/LPClose) pass None when unset."""
        assert validate_max_slippage_fraction(None) is None

    def test_error_names_the_field(self):
        with pytest.raises(ValueError, match="hedge_slippage"):
            validate_max_slippage_fraction(Decimal("1"), field_name="hedge_slippage")

    def test_error_type_is_honoured(self):
        """Call sites keep their own exception taxonomy; only the bound is shared."""
        with pytest.raises(InvalidBridgeError, match=r"must be in \[0, 1\)"):
            validate_max_slippage_fraction(Decimal("1"), error_type=InvalidBridgeError)

    def test_rejection_explains_why_rather_than_restating_the_bound(self):
        """A bare "out of range" invites a caller to just pass 0.999999 instead."""
        with pytest.raises(ValueError) as exc:
            validate_max_slippage_fraction(Decimal("1"))
        assert "zero" in str(exc.value)

    def test_guard_survives_python_dash_o(self):
        """``raise``, never ``assert`` — ``python -O`` strips assertions.

        Compiling the module with optimisation on and re-executing it must not
        change the guard's behaviour. A safety check that evaporates under an
        optimisation flag is not a safety check.
        """
        import inspect
        import textwrap

        src = textwrap.dedent(inspect.getsource(validate_max_slippage_fraction))
        namespace: dict = {"Decimal": Decimal}
        exec(compile(src, "<guard>", "exec", optimize=2), namespace)  # noqa: S102
        with pytest.raises(ValueError, match=r"must be in \[0, 1\)"):
            namespace["validate_max_slippage_fraction"](Decimal("1"))


# ---------------------------------------------------------------------------
# Reachability: the seven intent validators (VIB-6217)
# ---------------------------------------------------------------------------

#: (label, class, kwargs-without-slippage).
#: Every row raises ``ValidationError`` at construction: pydantic wraps ANY
#: ``ValueError`` raised from a model validator, and ``InvalidBridgeError`` /
#: ``InvalidEnsureBalanceError`` are both ``ValueError`` subclasses. The custom
#: types are therefore not observable through the pydantic constructor — they are
#: preserved for direct callers and for parity with the sibling field validators
#: in those modules (see ``test_error_type_is_honoured`` and
#: ``test_custom_error_types_are_not_observable_through_pydantic``).
_INTENT_CASES = [
    (
        "SwapIntent",
        SwapIntent,
        {"from_token": "USDC", "to_token": "WETH", "amount": Decimal("1")},
    ),
    (
        "LPOpenIntent",
        LPOpenIntent,
        {
            "pool": "USDC/WETH",
            "amount0": Decimal("1"),
            "amount1": Decimal("1"),
            "range_lower": Decimal("1"),
            "range_upper": Decimal("2"),
        },
    ),
    ("LPCloseIntent", LPCloseIntent, {"position_id": "123"}),
    (
        "BridgeIntent",
        BridgeIntent,
        {
            "token": "USDC",
            "amount": Decimal("1"),
            "from_chain": "base",
            "to_chain": "arbitrum",
        },
    ),
    (
        "EnsureBalanceIntent",
        EnsureBalanceIntent,
        {"token": "USDC", "min_amount": Decimal("1"), "target_chain": "base"},
    ),
    (
        "PerpOpenIntent",
        PerpOpenIntent,
        {
            "market": "ETH/USD",
            "collateral_token": "USDC",
            "collateral_amount": Decimal("100"),
            "size_usd": Decimal("100"),
            # gmx_v2 rejects leverage < 1.1 in a SEPARATE validator. Without this
            # the intent raises for a reason that has nothing to do with slippage
            # and the test below would pass with the bound removed entirely.
            "leverage": Decimal("2"),
        },
    ),
    (
        "PerpCloseIntent",
        PerpCloseIntent,
        {"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
    ),
]


class TestIntentValidatorsRejectFullSlippage:
    """All seven slippage-carrying intents share one bound and reject 100%.

    This is the reachable half of the change: a strategy constructing any of these
    with ``max_slippage=1`` is refused before the intent ever reaches a compiler.
    """

    @pytest.mark.parametrize(
        ("label", "cls", "kwargs"),
        _INTENT_CASES,
        ids=[c[0] for c in _INTENT_CASES],
    )
    def test_rejects_exactly_one(self, label, cls, kwargs):
        with pytest.raises(ValidationError) as exc:
            cls(**kwargs, max_slippage=Decimal("1"))
        # Assert the SLIPPAGE bound fired, not some unrelated validator.
        assert BOUND_MSG in str(exc.value), (
            f"{label} rejected max_slippage=1 for the wrong reason: {exc.value}"
        )

    @pytest.mark.parametrize(
        ("label", "cls", "kwargs"),
        _INTENT_CASES,
        ids=[c[0] for c in _INTENT_CASES],
    )
    def test_accepts_just_below_one(self, label, cls, kwargs):
        """The bound must be exclusive at 1, not a ceiling on tolerance generally."""
        intent = cls(**kwargs, max_slippage=Decimal("0.9999"))
        assert intent.max_slippage == Decimal("0.9999")

    @pytest.mark.parametrize(
        ("label", "cls", "kwargs"),
        _INTENT_CASES,
        ids=[c[0] for c in _INTENT_CASES],
    )
    def test_rejects_negative(self, label, cls, kwargs):
        with pytest.raises(ValidationError) as exc:
            cls(**kwargs, max_slippage=Decimal("-0.01"))
        assert BOUND_MSG in str(exc.value)

    def test_custom_error_types_are_not_observable_through_pydantic(self):
        """Recording a real constraint on "each site keeps its own exception type".

        ``InvalidBridgeError`` and ``InvalidEnsureBalanceError`` both subclass
        ``ValueError``, and pydantic wraps every ``ValueError`` raised inside a
        model validator into ``ValidationError``. So passing ``error_type=`` does
        NOT change what a caller of ``BridgeIntent(...)`` catches — it never did,
        including before this refactor. The custom type is preserved for direct
        (non-pydantic) callers and for parity with the sibling ``field_validator``
        s in those modules; it is not a behaviour difference at construction.
        """
        assert issubclass(InvalidBridgeError, ValueError)
        assert issubclass(InvalidEnsureBalanceError, ValueError)
        with pytest.raises(ValidationError):
            BridgeIntent(
                token="USDC",
                amount=Decimal("1"),
                from_chain="base",
                to_chain="arbitrum",
                max_slippage=Decimal("1"),
            )

    def test_default_slippage_is_unchanged_for_every_intent(self):
        """The bound change must not have moved any default. Byte-for-byte callers."""
        expected = {
            "SwapIntent": Decimal("0.005"),
            "BridgeIntent": Decimal("0.005"),
            "EnsureBalanceIntent": Decimal("0.005"),
            "PerpOpenIntent": Decimal("0.01"),
            "PerpCloseIntent": Decimal("0.01"),
        }
        for label, cls, kwargs in _INTENT_CASES:
            intent = cls(**kwargs)
            if label in expected:
                assert intent.max_slippage == expected[label], label
            else:
                # LPOpenIntent / LPCloseIntent are optional and default to None so
                # the connector's own default applies.
                assert intent.max_slippage is None, label


# ---------------------------------------------------------------------------
# slippage_bps_to_fraction (VIB-6218) — NOT yet production-reachable
# ---------------------------------------------------------------------------


class TestSlippageBpsToFraction:
    def test_rejects_exactly_ten_thousand(self):
        """10_000 bps == 100% == a zero minimum. The boundary case that matters."""
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 10000\)"):
            slippage_bps_to_fraction(MAX_SLIPPAGE_BPS_EXCLUSIVE, context="swap")

    def test_accepts_one_bps_below_the_bound(self):
        assert slippage_bps_to_fraction(9_999, context="swap") == Decimal("0.9999")

    def test_accepts_zero(self):
        assert slippage_bps_to_fraction(0, context="swap") == Decimal("0")

    def test_rejects_above_the_bound(self):
        with pytest.raises(UnprotectedTradeError):
            slippage_bps_to_fraction(10_001, context="swap")

    def test_rejects_negative(self):
        with pytest.raises(UnprotectedTradeError):
            slippage_bps_to_fraction(-1, context="swap")

    def test_typical_tolerance_converts_exactly(self):
        assert slippage_bps_to_fraction(50, context="swap") == Decimal("0.005")

    def test_context_is_surfaced_in_the_error(self):
        with pytest.raises(UnprotectedTradeError, match="gmx_v2 perp open"):
            slippage_bps_to_fraction(10_000, context="gmx_v2 perp open")


# ---------------------------------------------------------------------------
# require_protective_min (VIB-6218) — NOT yet production-reachable
# ---------------------------------------------------------------------------


class TestRequireProtectiveMin:
    def test_rejects_zero(self):
        """A zero minimum accepts any output, including near-total loss."""
        with pytest.raises(UnprotectedTradeError, match="must be > 0"):
            require_protective_min(0, context="swap encode")

    def test_rejects_negative(self):
        with pytest.raises(UnprotectedTradeError):
            require_protective_min(-1, context="swap encode")

    def test_passes_through_a_positive_minimum_unchanged(self):
        """It wraps an argument in place, so it must be an identity on the good path."""
        assert require_protective_min(1, context="swap encode") == 1
        assert require_protective_min(10**18, context="swap encode") == 10**18

    def test_error_is_a_value_error_subclass(self):
        """Callers catching ValueError at the encode boundary still see the refusal."""
        assert issubclass(UnprotectedTradeError, ValueError)

    def test_error_carries_structured_context(self):
        with pytest.raises(UnprotectedTradeError) as exc:
            require_protective_min(0, context="lp close encode")
        assert exc.value.context == "lp close encode"
        assert "0" in exc.value.detail

    def test_guard_survives_python_dash_o(self):
        """``raise``, never ``assert``: the guard must not vanish under ``-O``."""
        import inspect
        import textwrap

        src = textwrap.dedent(inspect.getsource(require_protective_min))
        namespace: dict = {"UnprotectedTradeError": UnprotectedTradeError}
        exec(compile(src, "<guard>", "exec", optimize=2), namespace)  # noqa: S102
        with pytest.raises(UnprotectedTradeError):
            namespace["require_protective_min"](0, context="swap encode")


# ---------------------------------------------------------------------------
# derive_min_out (VIB-6218) — NOT yet production-reachable
# ---------------------------------------------------------------------------


class TestDeriveMinOut:
    def test_applies_the_haircut_exactly_once(self):
        """Double-application is the sibling defect: expected*(1-s) not *(1-s)**2."""
        assert derive_min_out(1_000_000, Decimal("0.01"), context="swap") == 990_000

    def test_zero_slippage_returns_the_quote_unchanged(self):
        assert derive_min_out(1_000_000, Decimal("0"), context="swap") == 1_000_000

    def test_rejects_slippage_of_exactly_one(self):
        """The VIB-6217 bound, enforced here too — this is the amplifier's entry."""
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 1\)"):
            derive_min_out(1_000_000, Decimal("1"), context="swap")

    def test_accepts_slippage_just_below_one(self):
        assert derive_min_out(1_000_000, Decimal("0.9999"), context="swap") == 100

    def test_rejects_negative_slippage(self):
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 1\)"):
            derive_min_out(1_000_000, Decimal("-0.01"), context="swap")

    def test_rejects_a_zero_quote(self):
        """An unusable quote is a refusal, not a licence to encode zero."""
        with pytest.raises(UnprotectedTradeError, match="expected_out must be > 0"):
            derive_min_out(0, Decimal("0.01"), context="swap")

    def test_rejects_a_negative_quote(self):
        with pytest.raises(UnprotectedTradeError, match="expected_out must be > 0"):
            derive_min_out(-1, Decimal("0.01"), context="swap")

    def test_refuses_when_truncation_would_annihilate_the_minimum(self):
        """The integer-floor annihilation case: a tiny quote * a haircut floors to 0.

        ``1 * (1 - 0.005) == 0.995 -> int() -> 0``. Returning 0 here would be the
        exact "1 wei floor is not a floor" defect the module exists to stop, so the
        guard must refuse rather than hand back an unprotected minimum.
        """
        with pytest.raises(UnprotectedTradeError, match="must be > 0"):
            derive_min_out(1, Decimal("0.005"), context="swap")

    def test_smallest_quote_that_survives_a_haircut(self):
        """Just above the truncation cliff — proves the refusal is a cliff, not a floor."""
        assert derive_min_out(2, Decimal("0.005"), context="swap") == 1

    def test_truncates_rather_than_rounding_up(self):
        """Rounding up would encode a minimum the quote does not support."""
        assert derive_min_out(1_000_001, Decimal("0.5"), context="swap") == 500_000


class TestIntentCompilerDefaultLpSlippageIsValidatedNotClamped:
    """``IntentCompiler(default_lp_slippage=...)`` must reject, not clamp (VIB-6217).

    Found by an auditor on PR #3496 after the ``cl_math`` clamp was fixed: the
    constructor still did ``min(max(x, 0), 1)`` one layer up, so
    ``default_lp_slippage=Decimal("5")`` silently became exactly ``1`` — and a
    tolerance of 1 sizes every LP minimum at zero. The same fail-open, one layer
    higher, reached by a different door.

    Rejecting at construction also keeps the bad value away from
    ``compute_min_amount_out``, whose bare ``ValueError`` would bypass the
    compile-time safety-refusal handlers and be billed to the circuit breaker as
    an ordinary fault.
    """

    @staticmethod
    def _make(slippage=None):
        from almanak.framework.intents.compiler import IntentCompiler
        from almanak.framework.intents.compiler_models import IntentCompilerConfig

        kwargs = {"config": IntentCompilerConfig(allow_placeholder_prices=True)}
        if slippage is not None:
            kwargs["default_lp_slippage"] = slippage
        return IntentCompiler(**kwargs)

    @pytest.mark.parametrize("bad", ["1", "1.5", "5", "-0.1"])
    def test_out_of_range_default_is_rejected_not_clamped(self, bad):
        with pytest.raises(ValueError, match=r"default_lp_slippage must be in \[0, 1\)"):
            self._make(Decimal(bad))

    def test_the_shipped_default_still_constructs(self):
        """Regression guard: the shipped default is LEGAL under the validator.

        If this fails, the validator has broken every LP strategy in the repo
        rather than only rejecting worse-than-default values.

        ALM-3186 changed the value from ``0.99`` (VIB-6225's placeholder) to
        ``0.01``, a real price tolerance. Asserted against the constant rather
        than a literal so the two cannot drift, plus an explicit bound that the
        default is protective — a default back above 10% would be the old defect
        returning under a new number.
        """
        from almanak.framework.intents.compiler import LP_SLIPPAGE_DEFAULT

        assert self._make().default_lp_slippage == LP_SLIPPAGE_DEFAULT
        assert LP_SLIPPAGE_DEFAULT == Decimal("0.01")
        assert Decimal("0") < LP_SLIPPAGE_DEFAULT <= Decimal("0.1")

    def test_a_real_tolerance_is_accepted_unchanged(self):
        """And is NOT silently altered — the old code clamped, this must pass through."""
        assert self._make(Decimal("0.05")).default_lp_slippage == Decimal("0.05")
