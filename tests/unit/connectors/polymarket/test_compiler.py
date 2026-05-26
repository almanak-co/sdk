from __future__ import annotations

from unittest.mock import MagicMock

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.polymarket.compiler import PolymarketCompiler
from almanak.framework.connectors.polymarket.exceptions import PolymarketMarketNotResolvedError
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, PredictionRedeemIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

WALLET_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _ctx(adapter: MagicMock) -> BaseCompilerContext:
    gateway_client = MagicMock()
    gateway_client.is_connected = True
    return BaseCompilerContext(
        chain="polygon",
        wallet_address=WALLET_ADDRESS,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=MagicMock(),
        gateway_client=gateway_client,
        price_oracle={},
        cache={"polymarket_adapter": adapter},
        services=MagicMock(),
    )


def _redeem_intent() -> PredictionRedeemIntent:
    return PredictionRedeemIntent(market_id="will-btc-break-100k", outcome="YES")


def test_compile_redeem_converts_adapter_transactions() -> None:
    adapter = MagicMock()
    adapter.compile_intent.return_value = ActionBundle(
        intent_type=IntentType.PREDICTION_REDEEM.value,
        transactions=[
            {
                "to": "0x1111111111111111111111111111111111111111",
                "value": 0,
                "data": "0xredeem",
                "gas_estimate": 210_000,
                "description": "Redeem winning positions",
                "tx_type": "prediction_redeem",
            },
            {
                "to": "0x2222222222222222222222222222222222222222",
                "data": "0xclaim",
            },
        ],
        metadata={"protocol": "polymarket", "winning_outcome": "YES"},
    )

    result = PolymarketCompiler().compile(_ctx(adapter), _redeem_intent())

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle is adapter.compile_intent.return_value
    assert len(result.transactions) == 2
    assert result.transactions[0].to == "0x1111111111111111111111111111111111111111"
    assert result.transactions[0].gas_estimate == 210_000
    assert result.transactions[0].tx_type == "prediction_redeem"
    assert result.transactions[1].to == "0x2222222222222222222222222222222222222222"
    assert result.transactions[1].gas_estimate == 200_000
    assert result.transactions[1].description == "Redeem prediction market positions"
    assert result.transactions[1].tx_type == "redeem"
    assert result.total_gas_estimate == 410_000


def test_compile_redeem_returns_adapter_metadata_error() -> None:
    adapter = MagicMock()
    adapter.compile_intent.return_value = ActionBundle(
        intent_type=IntentType.PREDICTION_REDEEM.value,
        transactions=[],
        metadata={"error": "Market has no redeemable winning position"},
    )

    result = PolymarketCompiler().compile(_ctx(adapter), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert result.error == "Market has no redeemable winning position"


def test_compile_redeem_handles_unresolved_market() -> None:
    adapter = MagicMock()
    adapter.compile_intent.side_effect = PolymarketMarketNotResolvedError("will-btc-break-100k")

    result = PolymarketCompiler().compile(_ctx(adapter), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert "will-btc-break-100k" in (result.error or "")


def test_compile_redeem_handles_adapter_exception() -> None:
    adapter = MagicMock()
    adapter.compile_intent.side_effect = RuntimeError("gateway unavailable")

    result = PolymarketCompiler().compile(_ctx(adapter), _redeem_intent())

    assert result.status == CompilationStatus.FAILED
    assert result.error == "gateway unavailable"
