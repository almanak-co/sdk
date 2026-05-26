"""Unit tests for BridgeIntent compilation in IntentCompiler."""

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.bridge_base import BridgeQuote
from almanak.connectors._strategy_base.bridge_compiler import BridgeCompiler
from almanak.connectors._strategy_base.compiler_registry import get_compiler as get_connector_compiler
from almanak.framework.intents import BridgeIntent
from almanak.framework.intents.bridge_selector import (
    BridgeSelectionResult,
    NoBridgeAvailableError,
)
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)


class _MockBridge:
    name = "Across"

    def build_deposit_tx(self, quote: BridgeQuote, recipient: str) -> dict[str, object]:
        assert quote.token == "USDC"
        assert recipient.startswith("0x")
        return {
            "to": "0x2222222222222222222222222222222222222222",
            "value": 0,
            "data": "0xabcdef",
        }


class _MockSelector:
    def __init__(self, result: BridgeSelectionResult):
        self._result = result

    def select_bridge(self, **_: object) -> BridgeSelectionResult:
        return self._result


class _RaisingSelector:
    def select_bridge(self, **_: object) -> BridgeSelectionResult:
        raise NoBridgeAvailableError("No bridge supports this token/route")


def _make_compiler() -> IntentCompiler:
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1111111111111111111111111111111111111111",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


def test_bridge_compilers_are_registered_in_connector_registry() -> None:
    """Across and Stargate BRIDGE compilation is owned by the connector registry."""
    assert isinstance(get_connector_compiler("across"), BridgeCompiler)
    assert isinstance(get_connector_compiler("stargate"), BridgeCompiler)


def test_intent_compiler_bridge_methods_are_folded_out() -> None:
    """Bridge compile logic should not live on IntentCompiler after the fold."""
    assert not hasattr(IntentCompiler, "_compile_bridge")
    assert not hasattr(IntentCompiler, "_get_bridge_selector")


def test_compile_bridge_success_builds_action_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge intents compile into BRIDGE ActionBundle with required metadata."""
    compiler = _make_compiler()

    quote = BridgeQuote(
        bridge_name="Across",
        token="USDC",
        from_chain="base",
        to_chain="arbitrum",
        input_amount=Decimal("100"),
        output_amount=Decimal("99.8"),
        fee_amount=Decimal("0.2"),
        estimated_time_seconds=420,
        route_data={"amount_wei": "100000000"},
        quote_id="q-123",
    )
    selection = BridgeSelectionResult(
        bridge=_MockBridge(),
        quote=quote,
        selection_reasoning="mock selection",
    )
    monkeypatch.setattr(BridgeCompiler, "_build_selector", lambda self, ctx: _MockSelector(selection))

    intent = BridgeIntent(
        token="USDC",
        amount=Decimal("100"),
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.intent_type == "BRIDGE"

    metadata = result.action_bundle.metadata
    assert metadata["from_chain"] == "base"
    assert metadata["to_chain"] == "arbitrum"
    assert metadata["token"] == "USDC"
    assert metadata["amount"] == "100"
    assert metadata["bridge"] == "Across"
    assert metadata["estimated_time"] == 420
    assert metadata["fee"] == "0.2"
    assert metadata["is_cross_chain"] is True

    # ERC20 bridge includes approve + deposit tx
    assert len(result.transactions) == 2
    assert result.transactions[0].tx_type == "approve"
    assert result.transactions[1].tx_type == "bridge_deposit"


def test_compile_bridge_resolves_all_amount_from_chain_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge compiler resolves amount='all' to on-chain token balance for from_chain."""
    compiler = _make_compiler()

    # 500 USDC (6 decimals) on the from_chain
    balance_wei = 500_000_000  # 500 USDC

    quote = BridgeQuote(
        bridge_name="Across",
        token="USDC",
        from_chain="base",
        to_chain="arbitrum",
        input_amount=Decimal("500"),
        output_amount=Decimal("499.5"),
        fee_amount=Decimal("0.5"),
        estimated_time_seconds=420,
        route_data={"amount_wei": str(balance_wei)},
        quote_id="q-all",
    )
    selection = BridgeSelectionResult(
        bridge=_MockBridge(),
        quote=quote,
        selection_reasoning="mock selection",
    )
    monkeypatch.setattr(BridgeCompiler, "_build_selector", lambda self, ctx: _MockSelector(selection))
    # Simulate a 500 USDC balance on the from_chain (base)
    monkeypatch.setattr(compiler, "_query_erc20_balance_for_chain", lambda *_: balance_wei)

    intent = BridgeIntent(
        token="USDC",
        amount="all",
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.SUCCESS, f"Expected success, got: {result.error}"
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["amount"] == "500"


def test_compile_bridge_all_amount_fails_when_no_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge compiler fails when amount='all' and no balance on from_chain."""
    compiler = _make_compiler()
    monkeypatch.setattr(compiler, "_query_erc20_balance_for_chain", lambda *_: 0)

    intent = BridgeIntent(
        token="USDC",
        amount="all",
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "No USDC balance to bridge" in result.error


def test_compile_bridge_all_amount_fails_when_balance_query_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bridge compiler fails gracefully when RPC unavailable for amount='all' resolution."""
    compiler = _make_compiler()
    monkeypatch.setattr(compiler, "_query_erc20_balance_for_chain", lambda *_: None)

    intent = BridgeIntent(
        token="USDC",
        amount="all",
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "RPC unavailable" in result.error


def test_compile_bridge_all_amount_native_deducts_gas_reserve(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge compiler deducts gas reserve when amount='all' and token is native (e.g. ETH)."""
    compiler = _make_compiler()

    # 1.001 ETH in wei — after 0.001 gas reserve, should bridge 1.0 ETH
    eth_decimals = 18
    balance_wei = int(Decimal("1.001") * Decimal(10**eth_decimals))
    expected_amount_wei = int(Decimal("1.0") * Decimal(10**eth_decimals))

    class _MockNativeBridge:
        name = "Across"

        def build_deposit_tx(self, quote: BridgeQuote, recipient: str) -> dict[str, object]:
            assert quote.token == "ETH"
            assert recipient.startswith("0x")
            return {
                "to": "0x2222222222222222222222222222222222222222",
                "value": expected_amount_wei,
                "data": "0xabcdef",
            }

    quote = BridgeQuote(
        bridge_name="Across",
        token="ETH",
        from_chain="arbitrum",
        to_chain="base",
        input_amount=Decimal("1.0"),
        output_amount=Decimal("0.999"),
        fee_amount=Decimal("0.001"),
        estimated_time_seconds=420,
        route_data={"amount_wei": str(expected_amount_wei)},
        quote_id="q-native-all",
    )
    selection = BridgeSelectionResult(
        bridge=_MockNativeBridge(),
        quote=quote,
        selection_reasoning="mock selection",
    )
    monkeypatch.setattr(BridgeCompiler, "_build_selector", lambda self, ctx: _MockSelector(selection))
    monkeypatch.setattr(compiler, "_query_native_balance_for_chain", lambda *_: balance_wei)

    intent = BridgeIntent(
        token="ETH",
        amount="all",
        from_chain="arbitrum",
        to_chain="base",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.SUCCESS, f"Expected success, got: {result.error}"
    assert result.action_bundle is not None
    # Amount should be 1 ETH (after 0.001 gas reserve deduction from 1.001), not the full 1.001
    assert Decimal(result.action_bundle.metadata["amount"]) == Decimal("1")


def test_compile_bridge_fails_cleanly_for_unsupported_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unsupported bridge route/token should fail with explicit compiler error."""
    compiler = _make_compiler()
    monkeypatch.setattr(BridgeCompiler, "_build_selector", lambda self, ctx: _RaisingSelector())

    intent = BridgeIntent(
        token="USDC",
        amount=Decimal("100"),
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "No bridge supports this token/route" in result.error
