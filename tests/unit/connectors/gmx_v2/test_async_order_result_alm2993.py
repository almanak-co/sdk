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


def _keyed_order_created_data(*, order_type: int = 2, is_long: bool = True) -> str:
    """Encode OrderCreated exactly as the production EventEmitter emits it.

    The payload is ``(msgSender, eventName, EventUtils.EventLogData)`` — a
    keyed dynamic struct, NOT a flat word layout. Fixtures must enforce the
    production encoding: the flat-word shape masked the field-misread bug the
    2026-07-25 live keeper run exposed (market decoded as an ABI offset).
    """
    from eth_abi import encode as abi_encode

    from almanak.connectors.gmx_v2.receipt_parser import _EVENT_LOG_DATA_ABI_TYPE

    payload = abi_encode(
        ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
        [
            "0x" + "77" * 20,
            "OrderCreated",
            (
                (
                    [
                        ("account", "0x" + "88" * 20),
                        ("receiver", "0x" + "88" * 20),
                        ("market", _MARKET),
                        ("initialCollateralToken", _COLLATERAL),
                    ],
                    [("swapPath", [])],
                ),
                (
                    [
                        ("orderType", order_type),
                        ("sizeDeltaUsd", _SIZE_DELTA_USD * 10**30),
                        ("initialCollateralDeltaAmount", 10 * 10**6),
                    ],
                    [],
                ),
                ([], []),
                ([("isLong", is_long)], []),
                ([("key", bytes.fromhex(_ORDER_KEY[2:]))], []),
                ([], []),
                ([], []),
            ),
        ],
    )
    return "0x" + payload.hex()


def _order_created_log(*, key: str | None = _ORDER_KEY, order_type: int = 2, data: str | None = None) -> dict:
    topics = [_EVENT_LOG1_TOPIC, EVENT_TOPICS["OrderCreated"]]
    if key is not None:
        topics.append(key)
    return {
        "address": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
        "topics": topics,
        "data": data if data is not None else _keyed_order_created_data(order_type=order_type),
        "logIndex": 7,
    }


def _legacy_flat_words_data(*, order_type: int = 2) -> str:
    """The pre-2026-07-25 fixture shape: flat words that no live event carries."""
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
    return "0x" + "".join(f"{word:064x}" for word in words)


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
        assert order.is_long is True
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
                "is_long": True,
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


class TestGMXAsyncOrderKeyedPayloadFailClosed:
    def test_non_keyed_payload_yields_key_only_never_offset_garbage(self) -> None:
        """A payload that is not the keyed EventUtils struct must not produce fields.

        The legacy flat-word decode read ABI struct offsets as field values
        (market=0x…a0 — reproduced live 2026-07-25), poisoning the settlement
        delta check. The key (indexed topic) stays authoritative; every other
        field must be None so the barrier measures loudly instead of comparing
        against garbage.
        """
        log = _order_created_log(data=_legacy_flat_words_data())
        parsed = GMXv2ReceiptParser().extract_async_orders_result(_receipt([log]), intent_type="PERP_OPEN")

        assert isinstance(parsed, ExtractOk)
        order = parsed.value[0]
        assert order.order_id == _ORDER_KEY
        assert order.market is None
        assert order.collateral_token is None
        assert order.is_long is None
        assert order.size_delta_usd is None
        # Kind still resolves from the intent when the payload is unreadable.
        assert order.kind is AsyncOrderKind.INCREASE
