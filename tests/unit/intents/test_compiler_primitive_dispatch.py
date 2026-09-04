from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType

SUPPORTED_ROUTES = [
    (IntentType.SWAP, "_compile_swap", ()),
    (IntentType.LP_OPEN, "_compile_lp_open", ()),
    (IntentType.LP_CLOSE, "_compile_lp_close", ()),
    (IntentType.LP_COLLECT_FEES, "_compile_collect_fees", ()),
    (IntentType.BORROW, "_compile_borrow", ()),
    (IntentType.REPAY, "_compile_repay", ()),
    (IntentType.DELEVERAGE, "_compile_repay", ()),
    (IntentType.SUPPLY, "_compile_supply", ()),
    (IntentType.WITHDRAW, "_compile_withdraw", ()),
    (IntentType.PERP_OPEN, "_compile_perp_via_registry", ()),
    (IntentType.PERP_CLOSE, "_compile_perp_via_registry", ()),
    (IntentType.PERP_CANCEL_ORDER, "_compile_perp_via_registry", ()),
    (IntentType.PERP_WITHDRAW, "_compile_perp_via_registry", ()),
    (IntentType.HOLD, "_compile_hold", ()),
    (IntentType.FLASH_LOAN, "_compile_flash_loan", ()),
    (IntentType.STAKE, "_compile_staking_via_registry", ("STAKE",)),
    (IntentType.UNSTAKE, "_compile_staking_via_registry", ("UNSTAKE",)),
    (IntentType.PREDICTION_BUY, "_compile_prediction_via_registry", ()),
    (IntentType.PREDICTION_SELL, "_compile_prediction_via_registry", ()),
    (IntentType.PREDICTION_REDEEM, "_compile_prediction_via_registry", ()),
    (IntentType.BRIDGE, "_compile_bridge_via_registry", ()),
    (IntentType.VAULT_DEPOSIT, "_compile_vault_via_registry", ()),
    (IntentType.VAULT_REDEEM, "_compile_vault_via_registry", ()),
    (IntentType.ENSURE_BALANCE, "_compile_ensure_balance", ()),
    (IntentType.WRAP_NATIVE, "_compile_wrap_native", ()),
    (IntentType.UNWRAP_NATIVE, "_compile_unwrap_native", ()),
]

UNSUPPORTED_ROUTES = [
    IntentType.VAULT_REALLOCATE,
    IntentType.VAULT_MANAGE,
    IntentType.LIQUIDATE,
    IntentType.OPEN_CDP,
    IntentType.MINT_STABLE,
    IntentType.REPAY_STABLE,
    IntentType.CLOSE_CDP,
]


def _compiler() -> IntentCompiler:
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = "arbitrum"
    compiler.wallet_address = "0x0000000000000000000000000000000000000000"
    compiler._gateway_client = None
    compiler._using_placeholders = False
    compiler._placeholder_warning_logged = False
    return compiler


@pytest.fixture(autouse=True)
def passthrough_amount_resolution() -> None:
    with patch(
        "almanak.framework.intents.amount_resolver.resolve_amount_all",
        side_effect=lambda intent, **_kwargs: intent,
    ):
        yield


def test_dispatch_expectations_cover_every_intent_type() -> None:
    supported = {intent_type for intent_type, _method, _args in SUPPORTED_ROUTES}

    assert supported.isdisjoint(UNSUPPORTED_ROUTES)
    assert supported | set(UNSUPPORTED_ROUTES) == set(IntentType)


@pytest.mark.parametrize(
    ("intent_type", "method_name", "fixed_args"),
    SUPPORTED_ROUTES,
    ids=lambda value: value.value if isinstance(value, IntentType) else None,
)
def test_supported_primitive_dispatch_is_exact(
    monkeypatch: pytest.MonkeyPatch,
    intent_type: IntentType,
    method_name: str,
    fixed_args: tuple[str, ...],
) -> None:
    compiler = _compiler()
    intent = SimpleNamespace(intent_type=intent_type, intent_id="intent-1")
    expected = MagicMock(name="compilation_result")
    handler = MagicMock(return_value=expected)
    monkeypatch.setattr(compiler, method_name, handler)

    result = compiler._compile_intent(intent)

    assert result is expected
    handler.assert_called_once_with(intent, *fixed_args)


@pytest.mark.parametrize("intent_type", UNSUPPORTED_ROUTES, ids=lambda intent_type: intent_type.value)
def test_unsupported_primitive_returns_canonical_failure(intent_type: IntentType) -> None:
    intent = SimpleNamespace(intent_type=intent_type, intent_id="intent-unsupported")

    result = _compiler()._compile_intent(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == f"Intent type {intent_type.value} is not supported by the compiler"
    assert result.intent_id == "intent-unsupported"


class _FutureIntentType(Enum):
    FUTURE = "FUTURE"


@dataclass
class _UnhashableIntentType:
    value: str


@pytest.mark.parametrize("intent_type", [_FutureIntentType.FUTURE, _UnhashableIntentType("FUTURE")])
def test_unknown_enum_like_primitive_returns_canonical_failure(intent_type: object) -> None:
    intent = SimpleNamespace(intent_type=intent_type, intent_id="intent-future")

    result = _compiler()._compile_intent(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Intent type FUTURE is not supported by the compiler"
    assert result.intent_id == "intent-future"


def test_unhashable_unknown_primitive_with_placeholders_preserves_failure() -> None:
    compiler = _compiler()
    compiler._using_placeholders = True
    intent = SimpleNamespace(
        intent_type=_UnhashableIntentType("FUTURE"),
        intent_id="intent-future",
    )

    result = compiler._compile_intent(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Intent type FUTURE is not supported by the compiler"
    assert result.intent_id == "intent-future"
    assert compiler._placeholder_warning_logged is True


def test_malformed_string_primitive_preserves_attribute_error_result() -> None:
    intent = SimpleNamespace(intent_type="SWAP", intent_id="intent-malformed")

    result = _compiler()._compile_intent(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "'str' object has no attribute 'value'"
    assert result.intent_id == "intent-malformed"


def test_amount_resolution_precedes_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    compiler = _compiler()
    original = SimpleNamespace(intent_type=IntentType.REPAY, intent_id="intent-original")
    resolved = SimpleNamespace(intent_type=IntentType.REPAY, intent_id="intent-resolved")
    handler = MagicMock(return_value=MagicMock(name="compilation_result"))
    monkeypatch.setattr(compiler, "_compile_repay", handler)

    with patch(
        "almanak.framework.intents.amount_resolver.resolve_amount_all",
        return_value=resolved,
    ) as resolve_amount_all:
        result = compiler._compile_intent(original)

    assert result is handler.return_value
    resolve_amount_all.assert_called_once_with(
        original,
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000000",
        gateway_client=None,
    )
    handler.assert_called_once_with(resolved)


def test_bridge_missing_connector_preserves_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    compiler = _compiler()
    intent = SimpleNamespace(intent_type=IntentType.BRIDGE, intent_id="intent-bridge")
    monkeypatch.setattr(
        "almanak.framework.intents.compiler._bridge_registry_protocol",
        lambda _intent: "missing_bridge",
    )
    monkeypatch.setattr("almanak.framework.intents.compiler.get_connector_compiler", lambda _protocol: None)

    result = compiler._compile_intent(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "No connector compiler registered for bridge protocol missing_bridge"
    assert result.intent_id == "intent-bridge"


def test_bridge_connector_receives_context_and_same_intent(monkeypatch: pytest.MonkeyPatch) -> None:
    compiler = _compiler()
    intent = SimpleNamespace(intent_type=IntentType.BRIDGE, intent_id="intent-bridge")
    context = object()
    connector_compiler = MagicMock()
    connector_compiler.compile.return_value = MagicMock(name="compilation_result")
    monkeypatch.setattr(
        "almanak.framework.intents.compiler._bridge_registry_protocol",
        lambda _intent: "across",
    )
    monkeypatch.setattr(
        "almanak.framework.intents.compiler.get_connector_compiler",
        lambda _protocol: connector_compiler,
    )
    monkeypatch.setattr(compiler, "_build_compiler_context", MagicMock(return_value=context))

    result = compiler._compile_intent(intent)

    assert result is connector_compiler.compile.return_value
    compiler._build_compiler_context.assert_called_once_with("across", connector_compiler)
    connector_compiler.compile.assert_called_once_with(context, intent)


@pytest.mark.parametrize(
    "intent_type",
    [
        IntentType.STAKE,
        IntentType.UNSTAKE,
        IntentType.HOLD,
        IntentType.UNWRAP_NATIVE,
        IntentType.VAULT_DEPOSIT,
        IntentType.VAULT_REDEEM,
    ],
)
def test_price_irrelevant_primitives_suppress_placeholder_warning(
    monkeypatch: pytest.MonkeyPatch,
    intent_type: IntentType,
) -> None:
    compiler = _compiler()
    compiler._using_placeholders = True
    route = compiler._PRIMITIVE_COMPILER_ROUTES[intent_type]
    monkeypatch.setattr(compiler, route.method_name, MagicMock())

    compiler._compile_intent(SimpleNamespace(intent_type=intent_type, intent_id="intent-1"))

    assert compiler._placeholder_warning_logged is False


def test_price_relevant_primitive_logs_placeholder_warning_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    compiler = _compiler()
    compiler._using_placeholders = True
    monkeypatch.setattr(compiler, "_compile_swap", MagicMock())
    intent = SimpleNamespace(intent_type=IntentType.SWAP, intent_id="intent-1")

    compiler._compile_intent(intent)
    compiler._compile_intent(intent)

    assert compiler._placeholder_warning_logged is True
    assert (
        caplog.messages.count(
            "IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. "
            "This is only acceptable for unit tests."
        )
        == 1
    )
