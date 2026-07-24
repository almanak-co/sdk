"""ALM-2993 regression tests for authoritative GMX asynchronous order IDs."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.receipt_parser import EVENT_TOPICS, GMXv2ReceiptParser
from almanak.framework.execution.extract_result import (
    CriticalAccountingError,
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from almanak.framework.execution.extracted_data import AsyncOrderKind, AsyncOrderStatus
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.result_enricher import ResultEnricher

_ORDER_KEY = "0x" + "ab" * 32
_EVENT_LOG1_TOPIC = "0x" + "11" * 32
_TX_HASH = "0x" + "22" * 32
_MARKET = "0x" + "33" * 20
_COLLATERAL = "0x" + "44" * 20
_SIZE_DELTA_USD = 100


def _order_created_log(*, key: str | None = _ORDER_KEY, order_type: int = 2) -> dict:
    words = [
        0,
        0,
        int(_MARKET, 16),
        int(_COLLATERAL, 16),
        order_type,
        0,
        0,
        _SIZE_DELTA_USD * 10**30,
        0,
        0,
        0,
        0,
        0,
        0,
    ]
    topics = [_EVENT_LOG1_TOPIC, EVENT_TOPICS["OrderCreated"]]
    if key is not None:
        topics.append(key)
    return {
        "address": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
        "topics": topics,
        "data": "0x" + "".join(f"{word:064x}" for word in words),
        "logIndex": 7,
    }


def _receipt(logs: list[dict]) -> dict:
    return {
        "transactionHash": _TX_HASH,
        "blockNumber": 123,
        "status": 1,
        "logs": logs,
        "gasUsed": 200_000,
    }


def _gateway_result(receipts: list[dict]) -> GatewayExecutionResult:
    return GatewayExecutionResult(
        success=True,
        tx_hashes=[f"0x{i + 1:064x}" for i in range(len(receipts))],
        total_gas_used=200_000,
        receipts=receipts,
        execution_id="alm-2993",
    )


class TestGMXAsyncOrderExtraction:
    def test_valid_order_created_key_is_authoritative(self) -> None:
        parsed = GMXv2ReceiptParser().extract_async_orders_result(_receipt([_order_created_log()]))

        assert isinstance(parsed, ExtractOk)
        assert len(parsed.value) == 1
        order = parsed.value[0]
        assert order.protocol == "gmx_v2"
        assert order.order_id == _ORDER_KEY
        assert order.order_key == _ORDER_KEY
        assert order.status is AsyncOrderStatus.PENDING
        assert order.kind is AsyncOrderKind.INCREASE
        assert order.market == _MARKET
        assert order.collateral_token == _COLLATERAL
        assert order.is_long is False
        assert order.size_delta_usd == _SIZE_DELTA_USD

    def test_intent_type_is_authoritative_when_dynamic_event_payload_is_not_positionally_decodable(self) -> None:
        parsed = GMXv2ReceiptParser().extract_async_orders_result(
            _receipt([_order_created_log(order_type=2**255)]),
            intent_type="PERP_OPEN",
        )

        assert isinstance(parsed, ExtractOk)
        assert parsed.value[0].kind is AsyncOrderKind.INCREASE

    def test_receipt_without_order_created_event_is_missing(self) -> None:
        parsed = GMXv2ReceiptParser().extract_async_orders_result(_receipt([]))

        assert isinstance(parsed, ExtractMissing)
        assert parsed.reason == "no OrderCreated event"

    @pytest.mark.parametrize("key", [None, "0x1234", "0x" + "00" * 32])
    def test_missing_or_malformed_order_created_key_fails_closed(self, key: str | None) -> None:
        parsed = GMXv2ReceiptParser().extract_async_orders_result(_receipt([_order_created_log(key=key)]))

        assert isinstance(parsed, ExtractError)
        assert "exact non-zero bytes32 key" in parsed.error
        assert "log_index=7" in parsed.error


class TestGMXAsyncOrderResultEnrichment:
    @pytest.mark.parametrize(
        ("intent_type", "order_type", "expected_kind"),
        [
            ("PERP_OPEN", 2**255, AsyncOrderKind.INCREASE),
            ("PERP_CLOSE", 2**255, AsyncOrderKind.DECREASE),
        ],
    )
    def test_gateway_result_exposes_key_to_callbacks_and_structured_results(
        self,
        intent_type: str,
        order_type: int,
        expected_kind: AsyncOrderKind,
    ) -> None:
        result = _gateway_result(
            [
                {"status": 1, "logs": []},
                {"status": 1, "logs": [_order_created_log(order_type=order_type)]},
            ]
        )
        intent = SimpleNamespace(intent_type=intent_type, protocol="gmx_v2")
        context = SimpleNamespace(chain="arbitrum", protocol="gmx_v2")

        enriched = ResultEnricher().enrich(result, intent, context)

        assert len(enriched.async_orders) == 1
        assert enriched.async_orders[0].order_id == _ORDER_KEY
        assert enriched.async_orders[0].kind is expected_kind
        assert enriched.extracted_data["async_orders"] == enriched.async_orders
        assert enriched.to_outcome().async_orders == enriched.async_orders
        assert enriched.to_dict()["async_orders"] == [
            {
                "protocol": "gmx_v2",
                "order_id": _ORDER_KEY,
                "status": "pending",
                "kind": expected_kind.value,
                "market": _MARKET,
                "collateral_token": _COLLATERAL,
                "is_long": False,
                "size_delta_usd": str(_SIZE_DELTA_USD),
            }
        ]

    def test_required_order_event_missing_from_all_receipts_fails_closed(self) -> None:
        result = _gateway_result([{"status": 1, "logs": []}])
        intent = SimpleNamespace(intent_type="PERP_OPEN", protocol="gmx_v2")
        context = SimpleNamespace(chain="arbitrum", protocol="gmx_v2")

        with pytest.raises(CriticalAccountingError, match="required extraction missing") as exc_info:
            ResultEnricher().enrich(result, intent, context)

        assert exc_info.value.field_name == "async_orders"
        assert exc_info.value.protocol == "gmx_v2"

    def test_malformed_created_key_fails_closed_during_enrichment(self) -> None:
        result = _gateway_result([{"status": 1, "logs": [_order_created_log(key="0x1234")]}])
        intent = SimpleNamespace(intent_type="PERP_CLOSE", protocol="gmx_v2")
        context = SimpleNamespace(chain="arbitrum", protocol="gmx_v2")

        with pytest.raises(CriticalAccountingError, match="exact non-zero bytes32 key") as exc_info:
            ResultEnricher().enrich(result, intent, context)

        assert exc_info.value.field_name == "async_orders"
