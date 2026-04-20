"""Unit tests for BridgeIntent compilation in IntentCompiler."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.bridges.base import BridgeQuote
from almanak.framework.connectors.bridges.selector import (
    BridgeSelectionResult,
    NoBridgeAvailableError,
)
from almanak.framework.intents import BridgeIntent
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
    monkeypatch.setattr(compiler, "_get_bridge_selector", lambda: _MockSelector(selection))

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


def test_compile_bridge_rejects_unresolved_all_amount() -> None:
    """Bridge compiler fails deterministically for amount='all'."""
    compiler = _make_compiler()
    intent = BridgeIntent(
        token="USDC",
        amount="all",
        from_chain="base",
        to_chain="arbitrum",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "amount='all'" in result.error


def test_compile_bridge_fails_cleanly_for_unsupported_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unsupported bridge route/token should fail with explicit compiler error."""
    compiler = _make_compiler()
    monkeypatch.setattr(compiler, "_get_bridge_selector", lambda: _RaisingSelector())

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
