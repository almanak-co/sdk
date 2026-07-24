"""Branch coverage for StargateBridgeAdapter.get_quote and check_status.

get_quote is exercised with an injected fake TokenResolver and a patched
LayerZero fee seam — validation rejections, chain-id lookups, pool lookup,
fee math, route_data contents and error wrapping. check_status is exercised
against a mocked LayerZero scan API — status mapping, chain naming,
timestamps, hash fallbacks and error wrapping. No network.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
import requests

import almanak.connectors.stargate.adapter as stargate_adapter
from almanak.connectors._strategy_base.bridge_base import BridgeStatusEnum
from almanak.connectors.stargate.adapter import (
    StargateBridgeAdapter,
    StargateQuoteError,
    StargateStatusError,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError

DEPOSIT_TX = "0x" + "cd" * 32


class _FakeResolved:
    def __init__(self, decimals: int, address: str = "0xToken") -> None:
        self.decimals = decimals
        self.address = address


class _FakeResolver:
    def __init__(self, decimals: int = 6, error: Exception | None = None) -> None:
        self._decimals = decimals
        self._error = error
        self.calls: list[tuple[str, str]] = []

    def resolve(self, token: str, chain: str) -> _FakeResolved:
        self.calls.append((token, chain))
        if self._error is not None:
            raise self._error
        return _FakeResolved(self._decimals)


@pytest.fixture
def resolver():
    return _FakeResolver(decimals=6)


@pytest.fixture
def adapter(resolver):
    return StargateBridgeAdapter(token_resolver=resolver)


def _skip_validation(monkeypatch, adapter):
    """Bypass base-class validation to reach get_quote's own guard branches."""
    monkeypatch.setattr(adapter, "validate_transfer", lambda *args, **kwargs: (True, None))


class TestGetQuote:
    def test_unsupported_token_rejected(self, adapter):
        with pytest.raises(StargateQuoteError, match="Token DAI not supported by Stargate"):
            adapter.get_quote("DAI", Decimal("100"), "arbitrum", "optimism")

    def test_unsupported_route_rejected(self, adapter):
        with pytest.raises(StargateQuoteError, match="Route arbitrum -> arbitrum not supported"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "arbitrum")

    def test_validation_failure_without_message_uses_default(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "validate_transfer", lambda *args, **kwargs: (False, None))
        with pytest.raises(StargateQuoteError, match="Invalid transfer parameters"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "optimism")

    @pytest.mark.parametrize(
        ("from_chain", "to_chain"),
        [("fantom", "optimism"), ("arbitrum", "fantom")],
    )
    def test_unknown_layerzero_chain_rejected(self, adapter, monkeypatch, from_chain, to_chain):
        _skip_validation(monkeypatch, adapter)
        with pytest.raises(StargateQuoteError, match="Unsupported chain: fantom"):
            adapter.get_quote("USDC", Decimal("100"), from_chain, to_chain)

    @pytest.mark.parametrize("missing_chain", ["arbitrum", "optimism"])
    def test_missing_evm_chain_id_rejected(self, adapter, monkeypatch, missing_chain):
        _skip_validation(monkeypatch, adapter)
        monkeypatch.delitem(stargate_adapter.EVM_CHAIN_IDS, missing_chain)
        with pytest.raises(StargateQuoteError, match=f"Unsupported EVM chain: {missing_chain}"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "optimism")

    def test_token_without_pool_on_source_chain(self, adapter, monkeypatch):
        # Polygon has no ETH pool; base-class validation would reject the
        # route token first, so bypass it to reach the pool lookup guard.
        _skip_validation(monkeypatch, adapter)
        with pytest.raises(StargateQuoteError, match="Token ETH not supported on polygon"):
            adapter.get_quote("ETH", Decimal("1"), "polygon", "arbitrum")

    def test_happy_path_fee_and_route_data(self, adapter, resolver):
        quote = adapter.get_quote("USDC", Decimal("1000"), "arbitrum", "optimism")

        assert quote.bridge_name == "Stargate"
        assert quote.token == "USDC"
        assert quote.input_amount == Decimal("1000")
        # Protocol fee: 0.06% of 1000 = 0.6 USDC (relayer portion).
        assert quote.relayer_fee_amount == Decimal("0.6")
        # LZ fee: arbitrum base 0.001 * 3x safety = 0.003 (gas portion).
        assert quote.gas_fee_amount == Decimal("0.003")
        assert quote.fee_amount == Decimal("0.603")
        # Output only subtracts the token-denominated protocol fee.
        assert quote.output_amount == Decimal("999.4")
        assert quote.estimated_time_seconds == 60
        assert quote.slippage_tolerance == Decimal("0.005")

        assert quote.route_data == {
            "pool_address": "0xe8CDF27AcD73a434D661C84887215F7598e7d0d3",
            "from_lz_chain_id": 30110,
            "to_lz_chain_id": 30111,
            "from_evm_chain_id": 42161,
            "to_evm_chain_id": 10,
            "amount_wei": "1000000000",
            "min_amount_wei": "995000000",
            "lz_fee_wei": "3000000000000000",
            "token": "USDC",
        }
        assert quote.quote_id is not None
        assert quote.quote_id.startswith("sg_")
        assert quote.quote_id.endswith("_arbitrum_optimism")
        # Decimals resolved through the injected TokenResolver.
        assert resolver.calls == [("USDC", "arbitrum")]

    def test_chain_names_lowercased(self, adapter):
        quote = adapter.get_quote("USDC", Decimal("50"), "Arbitrum", "OPTIMISM")
        assert quote.from_chain == "arbitrum"
        assert quote.to_chain == "optimism"

    def test_token_resolution_error_propagates_unwrapped(self):
        error = TokenResolutionError(token="USDC", chain="arbitrum", reason="registry down")
        adapter = StargateBridgeAdapter(token_resolver=_FakeResolver(error=error))
        with pytest.raises(TokenResolutionError):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "optimism")

    def test_request_exception_wrapped(self, adapter, monkeypatch):
        def _boom(*args, **kwargs):
            raise requests.ConnectionError("dns failure")

        monkeypatch.setattr(adapter, "_estimate_layerzero_fee", _boom)
        with pytest.raises(StargateQuoteError, match="API request failed"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "optimism")

    @pytest.mark.parametrize("error", [ValueError("bad value"), KeyError("missing")])
    def test_calculation_error_wrapped(self, adapter, monkeypatch, error):
        def _boom(*args, **kwargs):
            raise error

        monkeypatch.setattr(adapter, "_estimate_layerzero_fee", _boom)
        with pytest.raises(StargateQuoteError, match="Failed to calculate quote"):
            adapter.get_quote("USDC", Decimal("100"), "arbitrum", "optimism")


@pytest.fixture
def status_adapter(adapter, monkeypatch):
    adapter._api_response = {}

    def _call_layerzero_api(endpoint, params=None):
        adapter.last_call = (endpoint, params)
        response = adapter._api_response
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(adapter, "_call_layerzero_api", _call_layerzero_api)
    return adapter


def _message(**overrides):
    payload = {
        "status": "INFLIGHT",
        "srcChainId": 30110,
        "dstChainId": 30111,
    }
    payload.update(overrides)
    return payload


class TestCheckStatus:
    def test_empty_deposit_id_raises(self, status_adapter):
        with pytest.raises(StargateStatusError, match="required"):
            status_adapter.check_status("")

    def test_unprefixed_hash_normalized(self, status_adapter):
        status_adapter._api_response = {"messages": []}
        status = status_adapter.check_status(DEPOSIT_TX[2:])
        assert status.bridge_deposit_id == DEPOSIT_TX
        assert status_adapter.last_call == (f"v1/messages/tx/{DEPOSIT_TX}", None)

    @pytest.mark.parametrize("response", [{}, {"messages": []}])
    def test_missing_messages_returns_pending_skeleton(self, status_adapter, response):
        status_adapter._api_response = response
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.status == BridgeStatusEnum.PENDING
        assert status.from_chain == ""
        assert status.to_chain == ""
        assert status.token == ""
        assert status.input_amount == Decimal("0")
        assert status.source_tx_hash == DEPOSIT_TX

    @pytest.mark.parametrize(
        ("lz_status", "expected"),
        [
            ("INFLIGHT", BridgeStatusEnum.IN_FLIGHT),
            ("DELIVERED", BridgeStatusEnum.COMPLETED),
            ("FAILED", BridgeStatusEnum.FAILED),
            ("BLOCKED", BridgeStatusEnum.FAILED),
            ("CONFIRMING", BridgeStatusEnum.DEPOSITED),
            ("inflight", BridgeStatusEnum.IN_FLIGHT),
            ("SOME_NEW_STATE", BridgeStatusEnum.PENDING),
        ],
    )
    def test_status_mapping(self, status_adapter, lz_status, expected):
        status_adapter._api_response = {"messages": [_message(status=lz_status)]}
        assert status_adapter.check_status(DEPOSIT_TX).status == expected

    def test_missing_status_defaults_to_pending(self, status_adapter):
        message = _message()
        del message["status"]
        status_adapter._api_response = {"messages": [message]}
        assert status_adapter.check_status(DEPOSIT_TX).status == BridgeStatusEnum.PENDING

    def test_chain_ids_resolve_to_names(self, status_adapter):
        status_adapter._api_response = {"messages": [_message()]}
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.from_chain == "arbitrum"
        assert status.to_chain == "optimism"

    def test_unknown_chain_ids_stringified(self, status_adapter):
        status_adapter._api_response = {"messages": [_message(srcChainId=12345, dstChainId=67890)]}
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.from_chain == "12345"
        assert status.to_chain == "67890"

    def test_missing_chain_ids_default_to_zero_string(self, status_adapter):
        message = _message()
        del message["srcChainId"]
        del message["dstChainId"]
        status_adapter._api_response = {"messages": [message]}
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.from_chain == "0"
        assert status.to_chain == "0"

    def test_timestamps_parsed_when_completed(self, status_adapter):
        status_adapter._api_response = {
            "messages": [
                _message(
                    status="DELIVERED",
                    created="2026-01-02T03:04:05Z",
                    updated="2026-01-02T03:06:07Z",
                )
            ]
        }
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.deposited_at == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert status.completed_at == datetime(2026, 1, 2, 3, 6, 7, tzinfo=UTC)

    def test_updated_ignored_when_not_completed(self, status_adapter):
        status_adapter._api_response = {
            "messages": [
                _message(
                    status="INFLIGHT",
                    created="2026-01-02T03:04:05Z",
                    updated="2026-01-02T03:06:07Z",
                )
            ]
        }
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.deposited_at == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert status.completed_at is None

    def test_invalid_timestamps_swallowed(self, status_adapter):
        status_adapter._api_response = {
            "messages": [_message(status="DELIVERED", created="not-a-date", updated="also-not-a-date")]
        }
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.status == BridgeStatusEnum.COMPLETED
        assert status.deposited_at is None
        assert status.completed_at is None

    def test_src_tx_hash_falls_back_to_query_hash(self, status_adapter):
        status_adapter._api_response = {"messages": [_message()]}
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.source_tx_hash == DEPOSIT_TX
        assert status.destination_tx_hash is None
        assert status.relay_id is None

    def test_message_hashes_and_relay_id_used(self, status_adapter):
        status_adapter._api_response = {"messages": [_message(srcTxHash="0xsrc", dstTxHash="0xdst", guid="guid-1")]}
        status = status_adapter.check_status(DEPOSIT_TX)
        assert status.source_tx_hash == "0xsrc"
        assert status.destination_tx_hash == "0xdst"
        assert status.relay_id == "guid-1"

    def test_first_message_wins(self, status_adapter):
        status_adapter._api_response = {"messages": [_message(status="DELIVERED"), _message(status="FAILED")]}
        assert status_adapter.check_status(DEPOSIT_TX).status == BridgeStatusEnum.COMPLETED

    def test_request_exception_wrapped(self, status_adapter):
        status_adapter._api_response = requests.ConnectionError("dns failure")
        with pytest.raises(StargateStatusError, match="API request failed"):
            status_adapter.check_status(DEPOSIT_TX)

    @pytest.mark.parametrize("error", [ValueError("bad json"), KeyError("missing")])
    def test_parse_error_wrapped(self, status_adapter, error):
        status_adapter._api_response = error
        with pytest.raises(StargateStatusError, match="Failed to parse status response"):
            status_adapter.check_status(DEPOSIT_TX)
