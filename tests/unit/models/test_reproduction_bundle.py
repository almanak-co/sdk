"""Serialization and file-loading contracts for reproduction bundles."""

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest

from almanak.framework.cli.replay import load_bundle_from_file
from almanak.framework.models.reproduction_bundle import (
    ActionBundle,
    MarketData,
    ReproductionBundle,
    TimelineEventSnapshot,
    TransactionReceipt,
)


def complete_bundle() -> ReproductionBundle:
    return ReproductionBundle(
        bundle_id="bundle-1",
        deployment_id="deployment:repro123456",
        failure_timestamp=datetime(2026, 8, 1, 12, 34, 56, tzinfo=UTC),
        block_number=123456,
        chain="arbitrum",
        persistent_state={"position": {"status": "open", "size": "1.5"}},
        config={"max_slippage": "0.005"},
        action_bundle=ActionBundle(
            intent_type="SWAP",
            transactions=[{"to": "0xabc", "value": 7, "data": "0x1234"}],
            metadata={"route": ["USDC", "WETH"]},
            sensitive_data={"private_key": "must-not-persist"},
        ),
        transaction_hash="0xtx",
        receipt=TransactionReceipt(
            transaction_hash="0xtx",
            block_number=123456,
            block_hash="0xblock",
            status=0,
            gas_used=210000,
            effective_gas_price=123,
            logs=[{"address": "0xlog", "topics": ["0xtopic"]}],
            contract_address="0xcontract",
            revert_reason="execution reverted: STF",
        ),
        market_data=MarketData(
            timestamp=datetime(2026, 8, 1, 12, 34, 55, tzinfo=UTC),
            token_prices={"WETH": Decimal("3456.789")},
            pool_liquidity={"USDC/WETH": Decimal("1234567.89")},
            gas_price=100,
            base_fee=90,
            priority_fee=10,
            oracle_prices={"ETH/USD": Decimal("3455.125")},
            oracle_timestamps={"ETH/USD": datetime(2026, 8, 1, 12, 34, 50, tzinfo=UTC)},
        ),
        events_before=[
            TimelineEventSnapshot(
                timestamp=datetime(2026, 8, 1, 12, 34, 54, tzinfo=UTC),
                event_type="INTENT_EXECUTION_FAILED",
                description="Swap reverted",
                tx_hash="0xtx",
                metadata={"attempt": 2},
            )
        ],
        tenderly_trace_url="https://dashboard.tenderly.co/tx/arbitrum/0xtx",
        revert_reason="STF",
        created_at=datetime(2026, 8, 1, 12, 35, tzinfo=UTC),
    )


def minimal_payload() -> dict:
    return {
        "bundle_id": "bundle-minimal",
        "deployment_id": "deployment:minimal123",
        "failure_timestamp": "2026-08-01T12:34:56+00:00",
        "block_number": 1,
        "chain": "arbitrum",
        "persistent_state": {},
        "config": {},
    }


def test_complete_payload_roundtrip_preserves_all_persisted_values() -> None:
    original = complete_bundle()

    payload = original.to_dict()
    restored = ReproductionBundle.from_dict(payload)

    assert restored.to_dict() == payload
    assert restored.action_bundle is not None
    assert restored.action_bundle.sensitive_data == {}
    assert "sensitive_data" not in payload["action_bundle"]
    assert restored.market_data is not None
    assert restored.market_data.token_prices == {"WETH": Decimal("3456.789")}
    assert restored.market_data.oracle_timestamps == {"ETH/USD": datetime(2026, 8, 1, 12, 34, 50, tzinfo=UTC)}


def test_minimal_payload_restores_optional_defaults() -> None:
    before = datetime.now(UTC)

    restored = ReproductionBundle.from_dict(minimal_payload())

    assert restored.action_bundle is None
    assert restored.transaction_hash is None
    assert restored.receipt is None
    assert restored.market_data is None
    assert restored.events_before == []
    assert restored.tenderly_trace_url is None
    assert restored.revert_reason is None
    assert before <= restored.created_at <= datetime.now(UTC)


def test_json_file_roundtrip_uses_the_same_model_contract(tmp_path) -> None:
    payload = complete_bundle().to_dict()
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    restored = load_bundle_from_file(path)

    assert restored.to_dict() == payload


def test_missing_bundle_file_preserves_error(tmp_path) -> None:
    path = tmp_path / "missing.json"

    with pytest.raises(FileNotFoundError) as exc_info:
        load_bundle_from_file(path)

    assert str(exc_info.value) == f"Bundle file not found: {path}"


def test_malformed_bundle_json_preserves_decoder_error(tmp_path) -> None:
    path = tmp_path / "bundle.json"
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_bundle_from_file(path)


@pytest.mark.parametrize(
    "field",
    [
        "bundle_id",
        "deployment_id",
        "failure_timestamp",
        "block_number",
        "chain",
        "persistent_state",
        "config",
    ],
)
def test_missing_required_field_preserves_key_error(field: str) -> None:
    payload = minimal_payload()
    del payload[field]

    with pytest.raises(KeyError) as exc_info:
        ReproductionBundle.from_dict(payload)

    assert exc_info.value.args == (field,)


def test_invalid_nested_receipt_preserves_key_error() -> None:
    payload = complete_bundle().to_dict()
    del payload["receipt"]["block_hash"]

    with pytest.raises(KeyError) as exc_info:
        ReproductionBundle.from_dict(payload)

    assert exc_info.value.args == ("block_hash",)


def test_invalid_timestamp_preserves_value_error() -> None:
    payload = minimal_payload()
    payload["failure_timestamp"] = "not-a-timestamp"

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        ReproductionBundle.from_dict(payload)


def test_invalid_market_decimal_preserves_invalid_operation() -> None:
    payload = complete_bundle().to_dict()
    payload["market_data"]["token_prices"]["WETH"] = "not-a-decimal"

    with pytest.raises(InvalidOperation):
        ReproductionBundle.from_dict(payload)
