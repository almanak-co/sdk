"""Branch coverage for AcrossBridgeAdapter.check_status.

Exercises the deposit/status parsing against a mocked Across API — status
mapping, fill promotion, amount decimal handling, chain naming, timestamps
and the error wrapping. No network.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
import requests

from almanak.connectors._strategy_base.bridge_base import BridgeStatusEnum
from almanak.connectors.across.adapter import AcrossBridgeAdapter, AcrossStatusError

DEPOSIT_TX = "0x" + "ab" * 32


@pytest.fixture
def adapter(monkeypatch):
    adapter = AcrossBridgeAdapter()
    adapter._api_responses = {}

    def _call_api(path, params=None):
        adapter.last_call = (path, params)
        response = adapter._api_responses
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(adapter, "_call_api", _call_api)
    return adapter


def _response(**overrides):
    payload = {
        "status": "pending",
        "deposit": {
            "inputToken": "0xUSDCtoken",
            "inputAmount": "1000000",
            "outputAmount": "999000",
            "originChainId": 42161,
            "destinationChainId": 8453,
        },
    }
    payload.update(overrides)
    return payload


class TestCheckStatus:
    def test_empty_deposit_id_raises(self, adapter):
        with pytest.raises(AcrossStatusError, match="required"):
            adapter.check_status("")

    def test_unprefixed_hash_is_normalized(self, adapter):
        adapter._api_responses = _response()
        status = adapter.check_status(DEPOSIT_TX[2:])
        assert status.bridge_deposit_id == DEPOSIT_TX
        assert adapter.last_call[1] == {"depositTxHash": DEPOSIT_TX}

    @pytest.mark.parametrize(
        ("across_status", "expected"),
        [
            ("pending", BridgeStatusEnum.PENDING),
            ("expired", BridgeStatusEnum.EXPIRED),
            ("slow_fill_requested", BridgeStatusEnum.IN_FLIGHT),
            ("some-new-status", BridgeStatusEnum.PENDING),
        ],
    )
    def test_status_mapping(self, adapter, across_status, expected):
        adapter._api_responses = _response(status=across_status)
        assert adapter.check_status(DEPOSIT_TX).status == expected

    def test_filled_without_fill_tx_stays_filled(self, adapter):
        adapter._api_responses = _response(status="filled")
        status = adapter.check_status(DEPOSIT_TX)
        assert status.status == BridgeStatusEnum.FILLED
        assert status.completed_at is None

    def test_filled_with_fill_tx_promoted_to_completed(self, adapter):
        adapter._api_responses = _response(
            status="filled",
            fillTxHash="0xfill",
            fillTime=1700000100,
            depositTime=1700000000,
        )
        status = adapter.check_status(DEPOSIT_TX)
        assert status.status == BridgeStatusEnum.COMPLETED
        assert status.destination_tx_hash == "0xfill"
        assert status.deposited_at == datetime.fromtimestamp(1700000000, tz=UTC)
        assert status.filled_at == datetime.fromtimestamp(1700000100, tz=UTC)
        assert status.completed_at == status.filled_at

    def test_usdc_amounts_use_six_decimals(self, adapter):
        adapter._api_responses = _response()
        status = adapter.check_status(DEPOSIT_TX)
        assert status.input_amount == Decimal("1")
        assert status.output_amount == Decimal("0.999")

    def test_eth_like_amounts_use_eighteen_decimals(self, adapter):
        adapter._api_responses = _response(
            deposit={
                "inputToken": "0xWETHtoken",
                "inputAmount": str(10**18),
                "originChainId": 1,
                "destinationChainId": 10,
            }
        )
        status = adapter.check_status(DEPOSIT_TX)
        assert status.input_amount == Decimal("1")
        assert status.output_amount is None

    def test_chain_ids_resolve_to_names(self, adapter):
        adapter._api_responses = _response()
        status = adapter.check_status(DEPOSIT_TX)
        assert status.from_chain == "arbitrum"
        assert status.to_chain == "base"

    def test_unknown_chain_ids_stringified(self, adapter):
        adapter._api_responses = _response(
            deposit={"originChainId": 999999, "destinationChainId": 888888}
        )
        status = adapter.check_status(DEPOSIT_TX)
        assert status.from_chain == "999999"
        assert status.to_chain == "888888"

    def test_missing_deposit_info_defaults(self, adapter):
        adapter._api_responses = {"status": "pending"}
        status = adapter.check_status(DEPOSIT_TX)
        assert status.input_amount == Decimal("0")
        assert status.output_amount is None
        assert status.token == ""

    def test_request_exception_wrapped(self, adapter):
        adapter._api_responses = requests.ConnectionError("dns failure")
        with pytest.raises(AcrossStatusError, match="API request failed"):
            adapter.check_status(DEPOSIT_TX)

    def test_parse_error_wrapped(self, adapter):
        # The real _call_api raises ValueError on malformed JSON bodies.
        adapter._api_responses = ValueError("invalid json body")
        with pytest.raises(AcrossStatusError, match="Failed to parse"):
            adapter.check_status(DEPOSIT_TX)


from almanak.connectors.across.adapter import (  # noqa: E402
    ACROSS_CHAIN_IDS,
    AcrossQuoteError,
)


class TestGetQuote:
    @pytest.fixture(autouse=True)
    def _stub_lookup(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "validate_transfer", lambda *a: (True, None))
        monkeypatch.setattr(
            adapter, "_get_token_address", lambda token, chain_id: "0xTOKEN"
        )
        monkeypatch.setattr(adapter, "_get_token_decimals", lambda token, chain_id: 6)
        monkeypatch.setattr(adapter, "estimate_completion_time", lambda f, t: 120)

    def _fees(self, *, total="1000000", lp="200000", gas="300000", capital="500000"):
        return {
            "totalRelayFee": {"total": total},
            "lpFee": {"total": lp},
            "relayerGasFee": {"total": gas},
            "relayerCapitalFee": {"total": capital},
            "timestamp": "1700000000",
            "quoteTimestamp": "1700000000",
        }

    def test_invalid_transfer_uses_error_message(self, adapter, monkeypatch):
        monkeypatch.setattr(
            adapter, "validate_transfer", lambda *a: (False, "amount too small")
        )
        with pytest.raises(AcrossQuoteError, match="amount too small"):
            adapter.get_quote("USDC", Decimal("1"), "arbitrum", "base")

    def test_invalid_transfer_default_message(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "validate_transfer", lambda *a: (False, None))
        with pytest.raises(AcrossQuoteError, match="Invalid transfer parameters"):
            adapter.get_quote("USDC", Decimal("1"), "arbitrum", "base")

    @pytest.mark.parametrize(
        ("from_chain", "to_chain", "unsupported"),
        [("notachain", "base", "notachain"), ("arbitrum", "notachain", "notachain")],
    )
    def test_unsupported_chain_rejected(self, adapter, from_chain, to_chain, unsupported):
        with pytest.raises(AcrossQuoteError, match=f"Unsupported chain: {unsupported}"):
            adapter.get_quote("USDC", Decimal("1"), from_chain, to_chain)

    def test_unsupported_token_rejected(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "_get_token_address", lambda token, chain_id: None)
        with pytest.raises(AcrossQuoteError, match="not supported on arbitrum"):
            adapter.get_quote("XYZ", Decimal("1"), "arbitrum", "base")

    def test_happy_path_fee_math(self, adapter):
        adapter._api_responses = self._fees()
        quote = adapter.get_quote("USDC", Decimal("100"), "Arbitrum", "Base")
        assert quote.input_amount == Decimal("100")
        # 100 USDC minus 1 USDC total relay fee
        assert quote.output_amount == Decimal("99")
        assert quote.fee_amount == Decimal("1")
        assert quote.gas_fee_amount == Decimal("0.3")
        assert quote.relayer_fee_amount == Decimal("0.7")  # capital + lp
        assert quote.from_chain == "arbitrum"
        assert quote.to_chain == "base"
        assert quote.estimated_time_seconds == 120
        assert quote.route_data["amount_wei"] == str(100 * 10**6)
        assert quote.route_data["from_chain_id"] == ACROSS_CHAIN_IDS["arbitrum"]
        _, params = adapter.last_call
        assert params["amount"] == str(100 * 10**6)

    def test_missing_destination_token_falls_back_to_source(self, adapter, monkeypatch):
        addresses = {ACROSS_CHAIN_IDS["arbitrum"]: "0xSRC", ACROSS_CHAIN_IDS["base"]: None}
        monkeypatch.setattr(
            adapter, "_get_token_address", lambda token, chain_id: addresses[chain_id]
        )
        adapter._api_responses = self._fees()
        adapter.get_quote("USDC", Decimal("100"), "arbitrum", "base")
        _, params = adapter.last_call
        assert params["outputToken"] == "0xSRC"

    def test_request_exception_wrapped(self, adapter):
        adapter._api_responses = requests.ConnectionError("dns failure")
        with pytest.raises(AcrossQuoteError, match="API request failed"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "base")

    def test_unparseable_fee_wrapped(self, adapter):
        adapter._api_responses = self._fees(total="not-a-number")
        with pytest.raises(AcrossQuoteError, match="Failed to parse quote response"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "base")
