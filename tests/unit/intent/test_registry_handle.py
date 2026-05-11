"""Unit tests for VIB-4192 / T06 — Intent.registry_handle reserved field.

Mirrors the UAT card at ``docs/internal/uat-cards/VIB-4192.md`` so that
the executable contract lives in pytest (CI-runnable, deterministic) and
not just in the smart-evaluator scripts. Each ``D``-step has a
corresponding ``test_d*`` function below; the docstring on each test
links back to the card section.

Acceptance criteria covered:

    AC #1 — registry_handle: str | None = None on base Intent dataclass:
        :func:`test_field_declared_once_via_runtime_introspection` (D3.F9)
    AC #2 — Validated via record_for(intent_type) (strict, NOT classify):
        :func:`test_strict_record_for_on_construction` (D3.F6)
        :func:`test_strict_record_for_on_emission` (D3.F10)
    AC #3 — No per-primitive intent class redeclares the field:
        :func:`test_field_declared_once_via_runtime_introspection` (D3.F9)
    AC #4 — Round-trip serialization preserves the field:
        :func:`test_round_trip_per_class`,
        :func:`test_round_trip_via_intent_factory_chokepoint`,
        :func:`test_round_trip_via_intent_sequence`,
        :func:`test_round_trip_via_serialize_result_parallel`,
        :func:`test_flash_loan_callback_round_trip` (D2.M2),
        :func:`test_full_per_class_matrix_with_handle_and_default_none` (D2.M1)
    AC #5 — Same-iteration duplicate handles do not raise (T06's contract).
            T14 (VIB-4200, shipped 2026-05-10) wires the collision-guard
            typed exception at the StateManager write site, not at intent
            construction:
        :func:`test_no_collision_guard_runtime` (D3.F7 Half A — T06 still asserts
            no construction-time guard)
        :func:`test_collision_guard_symbol_is_wired_post_t14` (D3.F7 Half B —
            asserts T14's symbol IS present, post-T14 inversion)
        :func:`test_factory_helpers_do_not_accept_registry_handle` (D3.F11)
"""

from __future__ import annotations

import inspect
import re
import subprocess
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pydantic
import pytest

from almanak.framework.intents.advanced_intents import (
    FlashLoanIntent,
    StakeIntent,
    UnstakeIntent,
    UnwrapNativeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WrapNativeIntent,
)
from almanak.framework.intents.base import (
    BaseIntent,
    assert_registry_handle_known,
)
from almanak.framework.intents.bridge import BridgeIntent
from almanak.framework.intents.ensure_balance import EnsureBalanceIntent
from almanak.framework.intents.lending_intents import (
    BorrowIntent,
    DeleverageIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.intents.prediction_intents import (
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)
from almanak.framework.intents.vocabulary import (
    AnyIntent,  # noqa: F401  -- exported via re-import
    CollectFeesIntent,
    HoldIntent,
    Intent,
    IntentSequence,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.primitives.taxonomy import UnknownIntentTypeError

# ---------------------------------------------------------------------------
# Per-class fixture matrix (mirrors D2.M1 in the UAT card).
# ---------------------------------------------------------------------------

ALL_INTENT_CLASSES: list[type[BaseIntent]] = [
    SwapIntent,
    LPOpenIntent,
    LPCloseIntent,
    CollectFeesIntent,
    HoldIntent,
    BridgeIntent,
    EnsureBalanceIntent,
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
    DeleverageIntent,
    PerpOpenIntent,
    PerpCloseIntent,
    PredictionBuyIntent,
    PredictionSellIntent,
    PredictionRedeemIntent,
    FlashLoanIntent,
    StakeIntent,
    UnstakeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WrapNativeIntent,
    UnwrapNativeIntent,
]


def _flash_loan_minimal_callback() -> SwapIntent:
    """Minimal SwapIntent used as a non-empty callback for FlashLoanIntent."""
    return SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount_usd=Decimal("10"),
        chain="arbitrum",
    )


# Per-class kwargs that, when paired with ``registry_handle="x"``, build a
# valid instance. Matches the D2.M1 CASES list in the card. The handle key
# is stamped per case (so each class gets a distinct value to detect cross-
# contamination during round-trips).
def _case_kwargs() -> list[tuple[type[BaseIntent], dict]]:
    return [
        (SwapIntent, dict(from_token="USDC", to_token="WETH",
                          amount_usd=Decimal("100"), chain="arbitrum",
                          registry_handle="d2_swap")),
        (LPOpenIntent, dict(pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                            amount0=Decimal("1"), amount1=Decimal("2000"),
                            range_lower=Decimal("1800"),
                            range_upper=Decimal("2200"),
                            protocol="uniswap_v3", chain="arbitrum",
                            registry_handle="d2_lpopen")),
        (LPCloseIntent, dict(position_id="12345", protocol="uniswap_v3",
                             chain="arbitrum",
                             registry_handle="d2_lpclose")),
        (CollectFeesIntent, dict(pool="WAVAX/USDC/20",
                                 protocol="traderjoe_v2",
                                 chain="avalanche",
                                 registry_handle="d2_collect")),
        (HoldIntent, dict(reason="test", chain="arbitrum",
                          registry_handle="d2_hold")),
        (BridgeIntent, dict(token="USDC", amount=Decimal("100"),
                            from_chain="base", to_chain="arbitrum",
                            registry_handle="d2_bridge")),
        (EnsureBalanceIntent, dict(token="USDC",
                                   min_amount=Decimal("100"),
                                   target_chain="arbitrum",
                                   registry_handle="d2_ensure")),
        (BorrowIntent, dict(protocol="aave_v3",
                            collateral_token="WETH",
                            collateral_amount=Decimal("1"),
                            borrow_token="USDC",
                            borrow_amount=Decimal("1000"),
                            chain="arbitrum",
                            registry_handle="d2_borrow")),
        (RepayIntent, dict(protocol="aave_v3", token="USDC",
                           amount=Decimal("100"), chain="arbitrum",
                           registry_handle="d2_repay")),
        (SupplyIntent, dict(protocol="aave_v3", token="USDC",
                            amount=Decimal("100"), chain="arbitrum",
                            registry_handle="d2_supply")),
        (WithdrawIntent, dict(protocol="aave_v3", token="USDC",
                              amount=Decimal("100"), chain="arbitrum",
                              registry_handle="d2_withdraw")),
        (DeleverageIntent, dict(protocol="aave_v3", token="USDC",
                                amount=Decimal("100"),
                                trigger_reason="health_factor_low",
                                chain="arbitrum",
                                registry_handle="d2_deleverage")),
        (PerpOpenIntent, dict(protocol="gmx_v2", market="ETH-USD",
                              collateral_token="USDC",
                              collateral_amount=Decimal("100"),
                              size_usd=Decimal("200"),
                              leverage=Decimal("2"), is_long=True,
                              chain="arbitrum",
                              registry_handle="d2_perpopen")),
        (PerpCloseIntent, dict(protocol="gmx_v2", market="ETH-USD",
                               collateral_token="USDC", is_long=True,
                               chain="arbitrum",
                               registry_handle="d2_perpclose")),
        (PredictionBuyIntent, dict(protocol="polymarket",
                                   market_id="0xabc", outcome="YES",
                                   amount_usd=Decimal("10"),
                                   max_price=Decimal("0.5"),
                                   chain="polygon",
                                   registry_handle="d2_predbuy")),
        (PredictionSellIntent, dict(protocol="polymarket",
                                    market_id="0xabc", outcome="YES",
                                    shares=Decimal("10"),
                                    min_price=Decimal("0.4"),
                                    chain="polygon",
                                    registry_handle="d2_predsell")),
        (PredictionRedeemIntent, dict(protocol="polymarket",
                                      market_id="0xabc",
                                      chain="polygon",
                                      registry_handle="d2_predredeem")),
        (FlashLoanIntent, dict(provider="aave", token="USDC",
                               amount=Decimal("1000"), chain="arbitrum",
                               callback_intents=[_flash_loan_minimal_callback()],
                               registry_handle="d2_flash")),
        (StakeIntent, dict(protocol="lido", token_in="ETH",
                           amount=Decimal("1"), chain="ethereum",
                           registry_handle="d2_stake")),
        (UnstakeIntent, dict(protocol="lido", token_in="stETH",
                             amount=Decimal("1"), chain="ethereum",
                             registry_handle="d2_unstake")),
        (VaultDepositIntent, dict(protocol="metamorpho",
                                  vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                                  amount=Decimal("100"),
                                  deposit_token="USDC",
                                  chain="ethereum",
                                  registry_handle="d2_vaultdeposit")),
        (VaultRedeemIntent, dict(protocol="metamorpho",
                                 vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                                 shares=Decimal("100"),
                                 deposit_token="USDC",
                                 chain="ethereum",
                                 registry_handle="d2_vaultredeem")),
        (WrapNativeIntent, dict(token="ETH", amount=Decimal("1"),
                                chain="arbitrum",
                                registry_handle="d2_wrap")),
        (UnwrapNativeIntent, dict(token="WETH", amount=Decimal("1"),
                                  chain="arbitrum",
                                  registry_handle="d2_unwrap")),
    ]


# ---------------------------------------------------------------------------
# D1 — Correctness
# ---------------------------------------------------------------------------


def test_default_none_attribute_and_serialize_round_trip() -> None:
    """D1.S2 — defaulted intent serialises with registry_handle=None and
    deserialises back without losing the None.
    """
    i = Intent.swap("USDC", "WETH", amount_usd=Decimal("1000"), chain="arbitrum")
    assert i.registry_handle is None
    d = i.serialize()
    assert "registry_handle" in d, sorted(d.keys())
    assert d["registry_handle"] is None
    j = SwapIntent.deserialize(d)
    assert j.registry_handle is None


def test_construct_with_handle_stores_value() -> None:
    """D1.S3 — direct construction with the handle stores the value verbatim."""
    i = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("1000"), chain="arbitrum",
        registry_handle="hedge_leg_long",
    )
    assert i.registry_handle == "hedge_leg_long"


def test_round_trip_per_class() -> None:
    """D1.S4 — per-class serialize/deserialize preserves the handle."""
    i = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("1000"), chain="arbitrum",
        registry_handle="hedge_leg_long",
    )
    d = i.serialize()
    assert d["registry_handle"] == "hedge_leg_long"
    j = SwapIntent.deserialize(d)
    assert j.registry_handle == "hedge_leg_long"


def test_round_trip_via_intent_factory_chokepoint() -> None:
    """D1.S5 — `Intent.serialize` / `Intent.deserialize` (the generic-dispatch
    chokepoint used by serialize_result) preserves the field.
    """
    i = LPOpenIntent(
        pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        amount0=Decimal("1"), amount1=Decimal("2000"),
        range_lower=Decimal("1800"), range_upper=Decimal("2200"),
        protocol="uniswap_v3", chain="arbitrum",
        registry_handle="hedge_leg_long",
    )
    d = Intent.serialize(i)
    assert d["registry_handle"] == "hedge_leg_long"
    j = Intent.deserialize(d)
    assert j.registry_handle == "hedge_leg_long"


def test_round_trip_via_intent_sequence() -> None:
    """D1.S6 — IntentSequence serialize/deserialize preserves per-intent handles."""
    a = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("1000"), chain="arbitrum",
        registry_handle="leg_a",
    )
    b = LPOpenIntent(
        pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        amount0=Decimal("1"), amount1=Decimal("2000"),
        range_lower=Decimal("1800"), range_upper=Decimal("2200"),
        protocol="uniswap_v3", chain="arbitrum",
        registry_handle="leg_b",
    )
    seq = Intent.sequence([a, b])
    d = seq.serialize()
    seq2 = IntentSequence.deserialize(d)
    assert seq2.intents[0].registry_handle == "leg_a"
    assert seq2.intents[1].registry_handle == "leg_b"


def test_round_trip_via_serialize_result_parallel() -> None:
    """D1.S7 — serialize_result/deserialize_result with PARALLEL + nested SEQUENCE."""
    a = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("1000"), chain="arbitrum",
        registry_handle="par_a",
    )
    b = LPOpenIntent(
        pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        amount0=Decimal("1"), amount1=Decimal("2000"),
        range_lower=Decimal("1800"), range_upper=Decimal("2200"),
        protocol="uniswap_v3", chain="arbitrum",
        registry_handle="par_b",
    )
    parallel = [a, Intent.sequence([b])]
    d = Intent.serialize_result(parallel)
    out = Intent.deserialize_result(d)
    assert out[0].registry_handle == "par_a"
    assert out[1].intents[0].registry_handle == "par_b"


# ---------------------------------------------------------------------------
# D2 — Scalability (every concrete intent class)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls,kwargs", _case_kwargs(), ids=lambda v: getattr(v, "__name__", ""))
def test_full_per_class_matrix_with_handle_and_default_none(
    cls: type[BaseIntent], kwargs: dict
) -> None:
    """D2.M1 — full round-trip on every concrete intent class through
    per-class + generic + default-None chokepoints. A class whose manual
    serialize() drops the field WILL fail this test — FlashLoanIntent's
    hand-rolled dict literal is the canonical regression vector this
    test guards.
    """
    expected_handle = kwargs["registry_handle"]
    instance = cls(**kwargs)
    assert instance.registry_handle == expected_handle

    # Per-class serialize/deserialize.
    d_per_class = instance.serialize()
    assert "registry_handle" in d_per_class, sorted(d_per_class.keys())
    assert d_per_class["registry_handle"] == expected_handle
    rebuilt_per_class = cls.deserialize(d_per_class)
    assert rebuilt_per_class.registry_handle == expected_handle

    # Generic Intent.serialize / Intent.deserialize.
    d_generic = Intent.serialize(instance)
    assert d_generic.get("registry_handle") == expected_handle
    rebuilt_generic = Intent.deserialize(d_generic)
    assert rebuilt_generic.registry_handle == expected_handle

    # Default-None: build the same class WITHOUT the handle, assert the
    # serialized dict carries `registry_handle: None` for schema stability.
    default_kwargs = {k: v for k, v in kwargs.items() if k != "registry_handle"}
    default_instance = cls(**default_kwargs)
    assert default_instance.registry_handle is None
    d_default = default_instance.serialize()
    assert "registry_handle" in d_default, sorted(d_default.keys())
    assert d_default["registry_handle"] is None
    default_rebuilt = cls.deserialize(d_default)
    assert default_rebuilt.registry_handle is None


def test_flash_loan_callback_round_trip() -> None:
    """D2.M2 — FlashLoanIntent's hand-rolled serialize must include
    registry_handle on both the outer and inner callback intents. A pure
    pydantic ``model_dump`` would emit it automatically, but FlashLoan
    builds a dict literal — this is the regression vector the card
    explicitly calls out.
    """
    inner = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("100"), chain="arbitrum",
        registry_handle="callback_inner",
    )
    outer = FlashLoanIntent(
        provider="aave", token="USDC",
        amount=Decimal("1000"), chain="arbitrum",
        callback_intents=[inner],
        registry_handle="callback_outer",
    )
    d = outer.serialize()
    assert d["registry_handle"] == "callback_outer"
    assert d["callback_intents"][0]["registry_handle"] == "callback_inner"
    rebuilt = FlashLoanIntent.deserialize(d)
    assert rebuilt.registry_handle == "callback_outer"
    assert rebuilt.callback_intents[0].registry_handle == "callback_inner"


# ---------------------------------------------------------------------------
# D3 — Robustness (no silent failure)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "   ", "\t", "\n  \n"])
def test_empty_or_whitespace_handle_rejected(bad: str) -> None:
    """D3.F1 — empty / whitespace-only handles raise at construction."""
    with pytest.raises((pydantic.ValidationError, ValueError)):
        SwapIntent(
            from_token="USDC", to_token="WETH",
            amount_usd=Decimal("1000"), chain="arbitrum",
            registry_handle=bad,
        )


@pytest.mark.parametrize("bad", [123, 1.5, ["a"], {"x": 1}, object()])
def test_non_string_handle_rejected(bad: object) -> None:
    """D3.F2 — non-string handles raise at construction."""
    with pytest.raises((pydantic.ValidationError, TypeError, ValueError)):
        SwapIntent(
            from_token="USDC", to_token="WETH",
            amount_usd=Decimal("1000"), chain="arbitrum",
            registry_handle=bad,  # type: ignore[arg-type]
        )


def test_handle_field_is_frozen() -> None:
    """D3.F3 — once constructed, registry_handle cannot be mutated."""
    i = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("1000"), chain="arbitrum",
        registry_handle="frozen_test",
    )
    with pytest.raises((pydantic.ValidationError, TypeError, AttributeError)):
        i.registry_handle = "mutated"  # type: ignore[misc]


# Same kwargs spread D3.F6 + D3.F10 use — across 4 base classes.
_D3_KW_BY_BASE: dict[type[BaseIntent], dict] = {
    SwapIntent: dict(from_token="USDC", to_token="WETH",
                     amount_usd=Decimal("100"), chain="arbitrum"),
    LPOpenIntent: dict(pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                       amount0=Decimal("1"), amount1=Decimal("2000"),
                       range_lower=Decimal("1800"),
                       range_upper=Decimal("2200"),
                       protocol="uniswap_v3", chain="arbitrum"),
    BorrowIntent: dict(protocol="aave_v3", collateral_token="WETH",
                       collateral_amount=Decimal("1"),
                       borrow_token="USDC",
                       borrow_amount=Decimal("1000"),
                       chain="arbitrum"),
    PerpOpenIntent: dict(protocol="gmx_v2", market="ETH-USD",
                         collateral_token="USDC",
                         collateral_amount=Decimal("100"),
                         size_usd=Decimal("200"),
                         leverage=Decimal("2"), is_long=True,
                         chain="arbitrum"),
}


def _poison(base_cls: type[BaseIntent]) -> type[BaseIntent]:
    """Return a per-base-class subtype whose intent_type is a fake enum value."""
    POISON_TOKEN = "DEFINITELY_NOT_A_REAL_INTENT_TYPE_TX9F2K"

    class _Fake(Enum):
        POISON = POISON_TOKEN

    class _Poisoned(base_cls):  # type: ignore[misc, valid-type]
        @property
        def intent_type(self):  # type: ignore[override]
            return _Fake.POISON

    _Poisoned.__name__ = f"_Poisoned_{base_cls.__name__}"
    return _Poisoned


@pytest.mark.parametrize("base_cls", list(_D3_KW_BY_BASE), ids=lambda c: c.__name__)
def test_strict_record_for_on_construction(base_cls: type[BaseIntent]) -> None:
    """D3.F6 — construction-side validator raises UnknownIntentTypeError
    (or wrapped pydantic.ValidationError) when registry_handle is set on
    an intent whose resolved intent_type is not in TAXONOMY. Spread
    across 4 base subtypes proves the validator lives on BaseIntent —
    not just on SwapIntent.
    """
    poisoned = _poison(base_cls)
    with pytest.raises((UnknownIntentTypeError, pydantic.ValidationError)) as exc_info:
        poisoned(**_D3_KW_BY_BASE[base_cls], registry_handle="should_be_rejected")

    exc = exc_info.value
    if isinstance(exc, UnknownIntentTypeError):
        return
    # Wrapped — verify at least one error entry carries an
    # UnknownIntentTypeError (D3.F6's strict-cause requirement).
    assert isinstance(exc, pydantic.ValidationError)
    found_strict = any(
        isinstance((err.get("ctx") or {}).get("error"), UnknownIntentTypeError)
        for err in exc.errors()
    )
    assert found_strict, exc.errors()


def test_strict_record_for_on_construction_null_intent_type() -> None:
    """D3.F6 null guard — if a pathological subclass returns None for
    intent_type, the validator must still raise (not silently accept).
    """
    class _NullIntent(SwapIntent):
        @property
        def intent_type(self):  # type: ignore[override]
            return None  # type: ignore[return-value]

    # Tightened (CodeRabbit PR #2205 review): only the strict raise paths
    # are acceptable. Unrelated AttributeError / TypeError / ValueError
    # would let an incidental crash mask a real silent-accept regression.
    with pytest.raises((UnknownIntentTypeError, pydantic.ValidationError)) as exc_info:
        _NullIntent(
            from_token="USDC", to_token="WETH",
            amount_usd=Decimal("100"), chain="arbitrum",
            registry_handle="should_also_reject",
        )
    exc = exc_info.value
    if isinstance(exc, UnknownIntentTypeError):
        return
    # pydantic.ValidationError must wrap an UnknownIntentTypeError cause —
    # same rule as the per-subtype strict-record_for test above.
    found_strict = any(
        isinstance((err.get("ctx") or {}).get("error"), UnknownIntentTypeError)
        for err in exc.errors()
    )
    assert found_strict, (
        f"_NullIntent raised pydantic.ValidationError but NO error entry "
        f"carries UnknownIntentTypeError as the cause: {exc.errors()}"
    )


# Result-shape variants for D3.F10 — exercised against each base class.
_VALID_PASSENGER = SwapIntent(
    from_token="USDC", to_token="WETH",
    amount_usd=Decimal("50"), chain="arbitrum",
    registry_handle="valid_handle",
)


def _build_bypassed(base_cls: type[BaseIntent]) -> BaseIntent:
    """Return a fresh poisoned-via-model_construct instance of base_cls."""
    base_kwargs = _D3_KW_BY_BASE[base_cls]
    ok = base_cls(**base_kwargs)
    poisoned = _poison(base_cls)
    return poisoned.model_construct(  # type: ignore[no-any-return]
        **base_kwargs,
        registry_handle="bypass_handle_should_not_emit",
        intent_id=ok.intent_id,
        created_at=ok.created_at,
    )


_SHAPES: list[tuple[str, callable]] = [
    ("single",                      lambda bad: bad),
    ("list_solo",                   lambda bad: [bad]),
    ("list_with_valid",             lambda bad: [_VALID_PASSENGER, bad]),
    ("sequence",                    lambda bad: Intent.sequence([bad])),
    ("parallel_with_nested_sequence",
                                    lambda bad: [_VALID_PASSENGER, Intent.sequence([bad])]),
]


@pytest.mark.parametrize("base_cls", list(_D3_KW_BY_BASE), ids=lambda c: c.__name__)
@pytest.mark.parametrize("shape_label,shape_fn", _SHAPES, ids=[s for s, _ in _SHAPES])
def test_strict_record_for_on_emission(
    base_cls: type[BaseIntent], shape_label: str, shape_fn
) -> None:
    """D3.F10 — Intent.serialize_result re-validates registry_handle on
    every intent in the result tree. Pydantic's documented model_construct
    bypass produces a poisoned instance; passing it through the documented
    decide-result emission chokepoint MUST raise UnknownIntentTypeError
    regardless of the result shape (single/list/sequence/nested) or which
    leaf intent class produced it. Matrix: 4 base classes × 5 shapes.
    """
    bad_instance = _build_bypassed(base_cls)
    with pytest.raises((UnknownIntentTypeError, pydantic.ValidationError)) as exc_info:
        Intent.serialize_result(shape_fn(bad_instance))

    exc = exc_info.value
    if isinstance(exc, UnknownIntentTypeError):
        return
    # ValidationError wrapping — must carry the strict cause.
    assert isinstance(exc, pydantic.ValidationError)
    found_strict = any(
        isinstance((err.get("ctx") or {}).get("error"), UnknownIntentTypeError)
        for err in exc.errors()
    )
    assert found_strict, exc.errors()


# ---------------------------------------------------------------------------
# AC #5 — no collision guard wired (T14 belongs to a separate later ticket)
# ---------------------------------------------------------------------------


def test_no_collision_guard_runtime() -> None:
    """D3.F7 Half A — duplicate handles in the three same-iteration
    chokepoints (construction, IntentSequence, serialize_result PARALLEL)
    must NOT raise. T14 wires the collision guard; T06 just reserves the
    field.
    """
    DUP = "duplicate_handle_test"
    a = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("100"), chain="arbitrum",
        registry_handle=DUP,
    )
    b = SwapIntent(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("200"), chain="arbitrum",
        registry_handle=DUP,
    )
    assert a.registry_handle == b.registry_handle == DUP
    assert a.intent_id != b.intent_id  # UUID defaults must not regress

    # IntentSequence
    seq = Intent.sequence([a, b], description="dup-handle sequence smoke")
    assert seq.intents[0].registry_handle == DUP
    assert seq.intents[1].registry_handle == DUP
    seq_d = seq.serialize()
    seq_back = IntentSequence.deserialize(seq_d)
    assert seq_back.intents[0].registry_handle == DUP
    assert seq_back.intents[1].registry_handle == DUP

    # PARALLEL via serialize_result
    parallel_d = Intent.serialize_result([a, b])
    assert parallel_d["type"] == "PARALLEL"
    items = parallel_d["items"]
    assert len(items) == 2
    assert items[0]["registry_handle"] == DUP
    assert items[1]["registry_handle"] == DUP
    out = Intent.deserialize_result(parallel_d)
    assert out[0].registry_handle == DUP
    assert out[1].registry_handle == DUP


# T14 (VIB-4200) shipped the collision-guard symbol on 2026-05-10 — see
# `almanak/framework/state/registry_errors.py:RegistryAutoCollisionError`.
# This test (D3.F7 Half B) was authored under T06 to *block* T14 work from
# leaking into the T06 PR before T14 was ready. Now that T14 has shipped,
# the guard's invariant has flipped: the symbol MUST be present (otherwise
# T14 would have been silently regressed). The other predicted names
# ("registry_uniqueness", "duplicate_handle", "_assert_unique_handle", …)
# were speculative — T14's actual implementation introduced
# ``RegistryAutoCollisionError`` only, so the post-T14 invariant is
# narrower than the pre-T14 prohibition.
def test_collision_guard_symbol_is_wired_post_t14() -> None:
    """D3.F7 Half B (post-T14 inversion) — ``RegistryAutoCollisionError``
    MUST exist in ``almanak/``: it was wired by VIB-4200 / T14 on
    2026-05-10. A regression that drops the symbol would silently revert
    the typed-error contract every other ticket in epic VIB-4185 depends
    on.

    The grep scans the entire ``almanak/`` tree for at least one
    occurrence of the canonical exception class. A future PR is welcome to
    rename / restructure as long as a typed exception with this exact
    name remains importable from
    ``almanak.framework.state.registry_errors`` (the import-side check
    below pins that surface explicitly).
    """
    # Side A — static grep proves the symbol is somewhere in almanak/.
    repo_root = Path(__file__).resolve().parents[3]
    almanak_dir = repo_root / "almanak"
    result = subprocess.run(
        ["grep", "-rEn", "RegistryAutoCollisionError", str(almanak_dir)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode != 2, (
        f"grep scan errored (exit code {result.returncode}); the static "
        f"guard cannot prove the symbol is present.\nstderr:\n{result.stderr}"
    )
    assert result.returncode == 0, (
        "RegistryAutoCollisionError NOT found in almanak/. T14 (VIB-4200) "
        "was supposed to ship it on 2026-05-10. A silent revert here would "
        "regress every downstream ticket in epic VIB-4185."
    )

    # Side B — import-side surface pin: the canonical import path must
    # resolve to a class. Catches the case where the symbol exists only
    # in a comment or docstring (grep hits) but is NOT importable.
    from almanak.framework.state.registry_errors import RegistryAutoCollisionError

    assert isinstance(RegistryAutoCollisionError, type)
    assert issubclass(RegistryAutoCollisionError, Exception)


# ---------------------------------------------------------------------------
# AC #1 + AC #3 — runtime introspection
# ---------------------------------------------------------------------------


def _first_class_to_define(cls: type, field_name: str) -> type | None:
    """Walk the MRO bottom-up; return the most-specific class that has
    ``field_name`` in its own ``__annotations__``.
    """
    for ancestor in cls.__mro__:
        own_annotations = ancestor.__dict__.get("__annotations__", {})
        if field_name in own_annotations:
            return ancestor
    return None


def test_field_declared_once_via_runtime_introspection() -> None:
    """D3.F9 — registry_handle is declared exactly once (on BaseIntent)
    and inherited by every concrete intent class. AC #1 + AC #3.

    Pydantic's MRO-aware model_fields populates per class with parent
    fields; the test walks each class's __annotations__ directly to
    detect any leaf-class redeclaration that would violate AC #3.
    """
    declarers: set[str] = set()
    for cls in ALL_INTENT_CLASSES:
        assert "registry_handle" in cls.model_fields, cls.__name__
        declarer = _first_class_to_define(cls, "registry_handle")
        assert declarer is not None, f"{cls.__name__}: no MRO ancestor declares it"
        declarers.add(declarer.__name__)

    assert declarers == {"BaseIntent"}, (
        f"registry_handle declared in MULTIPLE classes (AC #3 violated): "
        f"{sorted(declarers)}"
    )


# ---------------------------------------------------------------------------
# AC #5 — factory helpers do not accept registry_handle
# ---------------------------------------------------------------------------

# Non-factory utility methods on Intent — NOT subject to the negative claim.
_NOT_FACTORIES = frozenset({
    "sequence",
    "serialize",
    "deserialize",
    "serialize_result",
    "deserialize_result",
    "get_type",
    "get_chain",
    "get_amount_field",
    "validate_chain",
    "validate_chained_amounts",
    "is_sequence",
    "normalize_decide_result",
    "count_intents",
    "has_chained_amount",
    "set_resolved_amount",
})

# Per-helper minimal kwargs — keys must mechanically match the
# auto-enumerated factory set on Intent. test_factory_helpers_*_drift
# below enforces the sync; this dict + the auto-enumeration constitute
# the introspection-driven coverage check from D3.F11.
_FACTORY_BUILDERS: dict[str, dict] = {
    "swap": dict(from_token="USDC", to_token="WETH",
                 amount_usd=Decimal("100"), chain="arbitrum"),
    "lp_open": dict(pool="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                    amount0=Decimal("1"), amount1=Decimal("2000"),
                    range_lower=Decimal("1800"),
                    range_upper=Decimal("2200"),
                    protocol="uniswap_v3", chain="arbitrum"),
    "lp_close": dict(position_id="12345", protocol="uniswap_v3",
                     chain="arbitrum"),
    "collect_fees": dict(pool="WAVAX/USDC/20",
                         protocol="traderjoe_v2",
                         chain="avalanche"),
    "hold": dict(reason="test", chain="arbitrum"),
    "bridge": dict(token="USDC", amount=Decimal("100"),
                   from_chain="base", to_chain="arbitrum"),
    "ensure_balance": dict(token="USDC",
                           min_amount=Decimal("100"),
                           target_chain="arbitrum"),
    "supply": dict(protocol="aave_v3", token="USDC",
                   amount=Decimal("100"), chain="arbitrum"),
    "withdraw": dict(protocol="aave_v3", token="USDC",
                     amount=Decimal("100"), chain="arbitrum"),
    "borrow": dict(protocol="aave_v3", collateral_token="WETH",
                   collateral_amount=Decimal("1"),
                   borrow_token="USDC",
                   borrow_amount=Decimal("1000"),
                   chain="arbitrum"),
    "repay": dict(protocol="aave_v3", token="USDC",
                  amount=Decimal("100"), chain="arbitrum"),
    "deleverage": dict(protocol="aave_v3", token="USDC",
                       amount=Decimal("100"),
                       trigger_reason="health_factor_low",
                       chain="arbitrum"),
    "perp_open": dict(market="ETH-USD",
                      collateral_token="USDC",
                      collateral_amount=Decimal("100"),
                      size_usd=Decimal("200"),
                      is_long=True, leverage=Decimal("2"),
                      protocol="gmx_v2", chain="arbitrum"),
    "perp_close": dict(market="ETH-USD",
                       collateral_token="USDC",
                       is_long=True, protocol="gmx_v2",
                       chain="arbitrum"),
    "flash_loan": dict(provider="aave", token="USDC",
                       amount=Decimal("1000"),
                       callback_intents=[_flash_loan_minimal_callback()],
                       chain="arbitrum"),
    "stake": dict(protocol="lido", token_in="ETH",
                  amount=Decimal("1"), chain="ethereum"),
    "unstake": dict(protocol="lido", token_in="stETH",
                    amount=Decimal("1"), chain="ethereum"),
    "wrap": dict(token="ETH", amount=Decimal("1"),
                 chain="arbitrum"),
    "unwrap": dict(token="WETH", amount=Decimal("1"),
                   chain="arbitrum"),
    "vault_deposit": dict(protocol="metamorpho",
                          vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                          amount=Decimal("100"),
                          deposit_token="USDC",
                          chain="ethereum"),
    "vault_redeem": dict(protocol="metamorpho",
                         vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                         shares=Decimal("100"),
                         deposit_token="USDC",
                         chain="ethereum"),
    "prediction_buy": dict(market_id="0xabc", outcome="YES",
                           amount_usd=Decimal("10"),
                           max_price=Decimal("0.5"),
                           protocol="polymarket",
                           chain="polygon"),
    "prediction_sell": dict(market_id="0xabc", outcome="YES",
                            shares=Decimal("10"),
                            min_price=Decimal("0.4"),
                            protocol="polymarket",
                            chain="polygon"),
    "prediction_redeem": dict(market_id="0xabc",
                              protocol="polymarket",
                              chain="polygon"),
}


def _auto_enumerate_factories() -> list[str]:
    """Return Intent's OWN @staticmethod factory helpers.

    Walks ``Intent.__dict__`` rather than ``inspect.getmembers(Intent)``
    so inherited callables (``mro``, etc.) don't pollute the set and trip
    the BUILDERS-sync check on a clean branch. Filters on
    ``isinstance(value, staticmethod)`` to restrict to the documented
    factory surface — Intent's helpers are all @staticmethod by
    convention. (CodeRabbit PR #2205 review.)
    """
    return sorted(
        name
        for name, value in Intent.__dict__.items()
        if (
            not name.startswith("_")
            and isinstance(value, staticmethod)
            and name not in _NOT_FACTORIES
        )
    )


def test_factory_builders_dict_in_sync_with_intent_class() -> None:
    """D3.F11 — Builder dict must mechanically match the Intent class's
    auto-enumerated factory surface. New helpers added to vocabulary.py
    without updating this test (or without adding to NOT_FACTORIES if
    not author-facing) MUST fail this test.
    """
    auto = set(_auto_enumerate_factories())
    builders = set(_FACTORY_BUILDERS)
    missing = auto - builders
    extra = builders - auto
    assert not missing, (
        f"Auto-enumerated factories with no builder: {sorted(missing)}. "
        f"Add a row to _FACTORY_BUILDERS for each (or to _NOT_FACTORIES "
        f"if not author-facing)."
    )
    assert not extra, (
        f"_FACTORY_BUILDERS rows with no matching factory on Intent: "
        f"{sorted(extra)}. Remove the stale rows."
    )


@pytest.mark.parametrize("name", sorted(_FACTORY_BUILDERS))
def test_factory_helpers_accept_registry_handle(name: str) -> None:
    """D3.F11 — every Intent.* factory helper accepts the registry_handle
    kwarg and threads it through to the returned intent's
    ``registry_handle`` attribute (VIB-4285 / VIB-4185 factory UX
    completion).

    The inversion of the previous negative claim
    (``test_factory_helpers_do_not_accept_registry_handle``) — T06
    deferred this surface; the multi-position fixtures in
    ``strategies/accounting/`` made the deferred ergonomics painful in
    practice (model_copy workaround or direct dataclass construction),
    so the factory layer now plumbs the kwarg through to every
    dataclass constructor while ``BaseIntent``'s construction-time
    validator continues to enforce the empty/whitespace and TAXONOMY
    checks.
    """
    factory = getattr(Intent, name)
    base_kwargs = _FACTORY_BUILDERS[name]
    intent = factory(**base_kwargs, registry_handle="leg_alpha")
    assert getattr(intent, "registry_handle", None) == "leg_alpha", (
        f"Intent.{name}: factory accepted registry_handle kwarg but the "
        f"returned intent's .registry_handle is not 'leg_alpha'. Confirm "
        f"the factory passes the kwarg through to the dataclass constructor."
    )


@pytest.mark.parametrize("name", sorted(_FACTORY_BUILDERS))
def test_factory_helpers_reject_empty_registry_handle(name: str) -> None:
    """Construction-side validator still fires when the kwarg is reached
    through the factory layer: empty / whitespace-only handles raise.
    Guards against a future refactor that quietly swallows the value
    instead of plumbing it through.
    """
    factory = getattr(Intent, name)
    base_kwargs = _FACTORY_BUILDERS[name]
    with pytest.raises((ValueError, pydantic.ValidationError)):
        factory(**base_kwargs, registry_handle="   ")


# ---------------------------------------------------------------------------
# Helper-API smoke tests for the BaseIntent module surface.
# ---------------------------------------------------------------------------


def test_assert_registry_handle_known_noop_when_handle_is_none() -> None:
    """The emission helper short-circuits on intents without a handle."""
    i = SwapIntent(from_token="USDC", to_token="WETH",
                   amount_usd=Decimal("100"), chain="arbitrum")
    # No raise.
    assert_registry_handle_known(i)


def test_assert_registry_handle_known_raises_on_poison() -> None:
    """The emission helper raises UnknownIntentTypeError on a bypassed handle."""
    poisoned = _poison(SwapIntent)
    bad = poisoned.model_construct(
        from_token="USDC", to_token="WETH",
        amount_usd=Decimal("100"), chain="arbitrum",
        registry_handle="bypass",
        intent_id="x", created_at=__import__("datetime").datetime.now(),
    )
    with pytest.raises(UnknownIntentTypeError):
        assert_registry_handle_known(bad)


# ---------------------------------------------------------------------------
# Sanity: docstring contract match (not load-bearing — purely a guard
# against accidental drift between code and module docstring).
# ---------------------------------------------------------------------------


def test_base_intent_docstring_mentions_record_for_and_t14() -> None:
    """Light guard: the module docstring should still reference the
    record_for strict path and T14's later collision-guard ownership.
    Catches a drive-by docstring rewrite that loses the design rationale.
    """
    from almanak.framework.intents import base as base_mod

    src = base_mod.__doc__ or ""
    assert "record_for" in src
    assert re.search(r"\bT14\b|\bVIB-4197\b", src)
