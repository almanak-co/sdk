"""Tests for PaperTrader intent helper methods."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.paper.engine import PaperTrader
from almanak.framework.models.reproduction_bundle import ActionBundle


def _trader() -> PaperTrader:
    trader = PaperTrader.__new__(PaperTrader)
    trader._backtest_id = "intent-helper-test"
    return trader


class _IntentEnum(Enum):
    SWAP = "SWAP"


class _DictIntent:
    def to_dict(self) -> dict[str, object]:
        return {"type": "SWAP", "amount": "1"}


class _BadToDictIntent:
    def __init__(self) -> None:
        self.intent_type = _IntentEnum.SWAP
        self.amount = Decimal("1.25")
        self._private = "secret"

    def to_dict(self) -> list[str]:
        return ["not", "a", "dict"]


class _PublicAttrIntent:
    def __init__(self) -> None:
        self.intent_type = _IntentEnum.SWAP
        self.amount = Decimal("2.5")
        self._private = "hidden"


class PerpCloseIntent:
    pass


class HoldIntent:
    pass


class _CompileIntent:
    def __init__(self, result: ActionBundle | Exception) -> None:
        self.result = result

    def compile(self) -> ActionBundle:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_serialize_intent_uses_dict_to_dict_result() -> None:
    assert _trader()._serialize_intent(_DictIntent()) == {"type": "SWAP", "amount": "1"}


def test_serialize_intent_ignores_non_dict_to_dict_result() -> None:
    serialized = _trader()._serialize_intent(_BadToDictIntent())

    assert serialized == {"intent_type": "SWAP", "amount": "1.25"}


def test_serialize_intent_public_attrs_convert_decimals_and_enums() -> None:
    serialized = _trader()._serialize_intent(_PublicAttrIntent())

    assert serialized == {"intent_type": "SWAP", "amount": "2.5"}


def test_get_intent_type_from_enum_value_and_class_name() -> None:
    trader = _trader()

    assert trader._get_intent_type(SimpleNamespace(intent_type=IntentType.SUPPLY)) is IntentType.SUPPLY
    assert trader._get_intent_type(SimpleNamespace(intent_type=_IntentEnum.SWAP)) is IntentType.SWAP
    assert trader._get_intent_type(PerpCloseIntent()) is IntentType.PERP_CLOSE
    assert trader._get_intent_type(object()) is IntentType.UNKNOWN


def test_is_hold_intent_variants() -> None:
    trader = _trader()

    assert trader._is_hold_intent(None) is True
    assert trader._is_hold_intent(SimpleNamespace(intent_type=IntentType.HOLD)) is True
    assert trader._is_hold_intent(SimpleNamespace(intent_type="HOLD")) is True
    assert trader._is_hold_intent(HoldIntent()) is True
    assert trader._is_hold_intent(SimpleNamespace(intent_type=IntentType.SWAP)) is False


def test_compile_intent_prefers_intent_compile_method() -> None:
    bundle = ActionBundle(intent_type="SWAP")

    assert _trader()._compile_intent(_CompileIntent(bundle)) is bundle


def test_compile_intent_uses_intent_compiler_after_compile_method_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import almanak.framework.intents as intents_module

    bundle = ActionBundle(intent_type="SWAP")

    class _Compiler:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def compile(self, intent: object) -> object:
            assert isinstance(intent, _CompileIntent)
            return SimpleNamespace(status=SimpleNamespace(value="SUCCESS"), action_bundle=bundle)

    monkeypatch.setattr(intents_module, "IntentCompiler", _Compiler)

    trader = _trader()
    trader.config = SimpleNamespace(chain="arbitrum")
    trader._cached_prices = {"USDC": Decimal("1")}
    trader._orchestrator = SimpleNamespace(signer=SimpleNamespace(address="0xwallet"))
    trader.fork_manager = SimpleNamespace(is_running=True, get_rpc_url=lambda: "http://fork")

    assert trader._compile_intent(_CompileIntent(RuntimeError("boom"))) is bundle


def test_compile_intent_returns_none_when_compilers_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    import almanak.framework.intents as intents_module

    class _Compiler:
        def __init__(self, **kwargs: object) -> None:
            pass

        def compile(self, intent: object) -> object:
            return SimpleNamespace(status=SimpleNamespace(value="FAILED"), action_bundle=None)

    monkeypatch.setattr(intents_module, "IntentCompiler", _Compiler)

    trader = _trader()
    trader.config = SimpleNamespace(chain="arbitrum")
    trader._cached_prices = {}
    trader._orchestrator = None
    trader.fork_manager = SimpleNamespace(is_running=False, get_rpc_url=lambda: "http://fork")

    assert trader._compile_intent(object()) is None


def test_resolve_token_address_static_fallback_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    import almanak.framework.backtesting.paper.engine as engine_module

    monkeypatch.setattr(engine_module, "_get_resolver", lambda: None)
    monkeypatch.setattr(
        engine_module,
        "TOKEN_ADDRESSES",
        {"arbitrum": {"USDC.e": "0xUsdcE"}},
    )

    trader = _trader()
    trader.config = SimpleNamespace(chain="arbitrum")

    assert trader._resolve_token_address("ETH") is None
    assert trader._resolve_token_address("usdc.E") == "0xUsdcE"
    assert trader._resolve_token_address("MISSING") is None


def test_get_actual_amount_out_prefers_swap_target_then_sums_fallback() -> None:
    trader = _trader()

    swap_intent = SimpleNamespace(intent_type=IntentType.SWAP, to_token="USDC")
    assert trader._get_actual_amount_out({"USDC": Decimal("10"), "WETH": Decimal("1")}, swap_intent) == Decimal("10")
    assert trader._get_actual_amount_out({"wrapped-USDC": Decimal("7")}, swap_intent) == Decimal("7")
    assert trader._get_actual_amount_out({"DAI": Decimal("3"), "WETH": Decimal("2")}, swap_intent) == Decimal("5")
    assert trader._get_actual_amount_out({}, swap_intent) is None


def test_snapshot_balances_preserves_intent_token_casing() -> None:
    trader = _trader()
    trader.portfolio_tracker = SimpleNamespace(current_balances={}, initial_balances={})

    async def _rpc_call(method: str, params: list[object]) -> str:
        assert method == "eth_getBalance"
        return "0x0"

    async def _get_token_balance(token_address: str, wallet_address: str) -> int:
        assert token_address == "0xToken"
        assert wallet_address == "0xwallet"
        return 123

    trader.fork_manager = SimpleNamespace(
        _rpc_call=_rpc_call,
        _get_token_balance=_get_token_balance,
    )
    trader._resolve_token_address = lambda symbol: "0xToken" if symbol.upper() == "USDC.E" else None

    balances = asyncio.run(
        trader._snapshot_balances(
            "0xwallet",
            intent=SimpleNamespace(to_token="USDC.e"),
        )
    )

    assert balances == {"ETH": 0, "USDC.e": 123}
