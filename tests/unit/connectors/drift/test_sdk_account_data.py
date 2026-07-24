"""Unit tests for DriftSDK on-chain account-data fetching.

Focus: ``DriftSDK._fetch_account_data`` — every branch of both transports
(gateway gRPC and deprecated direct RPC) plus the parse-error handling — and
the cheap read helpers layered on top of it (``fetch_market_oracle``,
``fetch_spot_market_oracle``, ``fetch_user_account`` / ``_parse_user_account``,
``build_remaining_accounts``).

Mocks sit at the narrowest seam the implementation uses:
- gateway transport: ``gateway_client.rpc.Call``
- direct RPC transport: ``session.post``

No sockets are ever opened.
"""

import base64
import json
import logging
import struct
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests
from solders.pubkey import Pubkey

from almanak.connectors.drift.constants import (
    MAX_PERP_POSITIONS,
    MAX_SPOT_POSITIONS,
    PERP_MARKET_ORACLE_OFFSET,
    PERP_POSITION_SIZE,
    SPOT_MARKET_ORACLE_OFFSET,
    SPOT_POSITION_SIZE,
    USER_AUTHORITY_OFFSET,
    USER_PERP_POSITIONS_OFFSET,
    USER_SPOT_POSITIONS_OFFSET,
)
from almanak.connectors.drift.exceptions import DriftMarketError
from almanak.connectors.drift.models import (
    DriftPerpPosition,
    DriftSpotPosition,
    DriftUserAccount,
)
from almanak.connectors.drift.sdk import DriftSDK

# A valid Solana pubkey for testing (matches test_drift_sdk.py).
TEST_WALLET = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
OTHER_WALLET = "11111111111111111111111111111112"
# Never contacted — session.post is always mocked before any call.
RPC_URL = "http://127.0.0.1:1/never-contacted"
TEST_ADDRESS = "So11111111111111111111111111111111111111112"

ORACLE = Pubkey.from_string(OTHER_WALLET)


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


def _gateway_response(success: bool = True, result: str = "", error: str = ""):
    """Stub for the gateway RpcResponse the SDK reads (success/result/error)."""
    return SimpleNamespace(success=success, result=result, error=error)


def _gateway_sdk(response) -> tuple[DriftSDK, MagicMock]:
    gateway = MagicMock()
    gateway.rpc.Call.return_value = response
    sdk = DriftSDK(wallet_address=TEST_WALLET, gateway_client=gateway)
    return sdk, gateway


def _rpc_sdk() -> DriftSDK:
    return DriftSDK(wallet_address=TEST_WALLET, rpc_url=RPC_URL)


def _account_info_result(data_field) -> str:
    """JSON the gateway returns: the raw JSON-RPC "result" context."""
    return json.dumps({"value": {"data": data_field}})


def _user_account_buffer(
    authority: str = TEST_WALLET,
    perp_base_amount: int = 0,
    perp_market_index: int = 0,
    spot_scaled_balance: int = 0,
    spot_market_index: int = 0,
) -> bytearray:
    """Build a full-size Drift User account buffer with slot 0 populated."""
    size = USER_SPOT_POSITIONS_OFFSET + MAX_SPOT_POSITIONS * SPOT_POSITION_SIZE
    data = bytearray(size)
    data[USER_AUTHORITY_OFFSET : USER_AUTHORITY_OFFSET + 32] = bytes(Pubkey.from_string(authority))
    struct.pack_into("<q", data, USER_PERP_POSITIONS_OFFSET, perp_base_amount)
    struct.pack_into("<q", data, USER_PERP_POSITIONS_OFFSET + 8, -1_000_000)
    struct.pack_into("<q", data, USER_PERP_POSITIONS_OFFSET + 16, 42)
    struct.pack_into("<H", data, USER_PERP_POSITIONS_OFFSET + 24, perp_market_index)
    data[USER_PERP_POSITIONS_OFFSET + 72] = 3
    struct.pack_into("<Q", data, USER_SPOT_POSITIONS_OFFSET, spot_scaled_balance)
    struct.pack_into("<H", data, USER_SPOT_POSITIONS_OFFSET + 8, spot_market_index)
    data[USER_SPOT_POSITIONS_OFFSET + 10] = 1
    data[USER_SPOT_POSITIONS_OFFSET + 11] = 2
    return data


class TestFetchAccountDataNoTransport:
    """Neither gateway_client nor rpc_url configured."""

    def test_returns_none_without_any_transport(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        assert sdk.session is None
        assert sdk._fetch_account_data(TEST_ADDRESS) is None


class TestFetchAccountDataGateway:
    """Gateway gRPC transport (gateway_client.rpc.Call seam)."""

    def test_success_decodes_base64_data(self):
        raw = b"drift-account-bytes"
        sdk, gateway = _gateway_sdk(_gateway_response(result=_account_info_result([_b64(raw), "base64"])))

        assert sdk._fetch_account_data(TEST_ADDRESS) == raw

        gateway.rpc.Call.assert_called_once()
        request = gateway.rpc.Call.call_args.args[0]
        assert request.chain == "solana"
        assert request.method == "getAccountInfo"
        assert request.id == "1"
        params = json.loads(request.params)
        assert params[0] == TEST_ADDRESS
        assert params[1] == {"encoding": "base64", "commitment": "confirmed"}
        assert gateway.rpc.Call.call_args.kwargs == {"timeout": sdk.timeout}

    def test_no_session_is_created_with_gateway(self):
        sdk, _ = _gateway_sdk(_gateway_response())
        assert sdk.session is None

    @pytest.mark.parametrize(
        ("response", "reason"),
        [
            (_gateway_response(success=False, error="boom"), "rpc-call-failed"),
            (_gateway_response(result=""), "empty-result"),
            (_gateway_response(result="[1, 2]"), "non-dict-result"),
            (_gateway_response(result="{}"), "missing-value"),
            (_gateway_response(result=json.dumps({"value": None})), "null-value"),
            (
                _gateway_response(result=_account_info_result([None, "base64"])),
                "null-data-entry",
            ),
            (_gateway_response(result=json.dumps({"value": {}})), "missing-data-key"),
            (_gateway_response(result="not-json{"), "invalid-json"),
        ],
        ids=lambda p: p if isinstance(p, str) else "",
    )
    def test_degraded_responses_return_none(self, response, reason):
        sdk, _ = _gateway_sdk(response)
        assert sdk._fetch_account_data(TEST_ADDRESS) is None


class TestFetchAccountDataDirectRpc:
    """Deprecated direct-RPC transport (session.post seam)."""

    def _response(self, body):
        resp = MagicMock()
        resp.json.return_value = body
        resp.raise_for_status.return_value = None
        return resp

    def test_session_is_configured_for_json_posts(self):
        sdk = _rpc_sdk()
        assert sdk.session is not None
        assert sdk.session.headers["Content-Type"] == "application/json"

    def test_success_decodes_base64_data(self):
        sdk = _rpc_sdk()
        raw = b"\x00\x01drift"
        body = {"result": {"value": {"data": [_b64(raw), "base64"]}}}
        with patch.object(sdk.session, "post", return_value=self._response(body)) as post:
            assert sdk._fetch_account_data(TEST_ADDRESS) == raw

        post.assert_called_once()
        assert post.call_args.args == (RPC_URL,)
        payload = post.call_args.kwargs["json"]
        assert payload["method"] == "getAccountInfo"
        assert payload["params"][0] == TEST_ADDRESS
        assert post.call_args.kwargs["timeout"] == sdk.timeout

    @pytest.mark.parametrize(
        ("body", "reason"),
        [
            ({"result": {"value": None}}, "account-missing"),
            ({}, "missing-result-key"),
            ({"result": {"value": {"data": [None, "base64"]}}}, "null-data-entry"),
            ({"result": {"value": {}}}, "missing-data-key"),
        ],
        ids=lambda p: p if isinstance(p, str) else "",
    )
    def test_degraded_bodies_return_none(self, body, reason):
        sdk = _rpc_sdk()
        with patch.object(sdk.session, "post", return_value=self._response(body)):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None

    def test_connection_error_returns_none(self):
        sdk = _rpc_sdk()
        with patch.object(
            sdk.session,
            "post",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None

    def test_http_error_status_returns_none(self):
        sdk = _rpc_sdk()
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        with patch.object(sdk.session, "post", return_value=resp):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None

    def test_invalid_base64_padding_returns_none(self):
        # "abc" has 3 data characters — binascii.Error on decode.
        sdk = _rpc_sdk()
        body = {"result": {"value": {"data": ["abc", "base64"]}}}
        with patch.object(sdk.session, "post", return_value=self._response(body)):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None

    def test_non_subscriptable_data_field_returns_none(self):
        # "data": 42 -> TypeError on [0] — swallowed by the parse-error handler.
        sdk = _rpc_sdk()
        body = {"result": {"value": {"data": 42}}}
        with patch.object(sdk.session, "post", return_value=self._response(body)):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None

    def test_unparseable_json_body_returns_none(self):
        sdk = _rpc_sdk()
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("no json")
        with patch.object(sdk.session, "post", return_value=resp):
            assert sdk._fetch_account_data(TEST_ADDRESS) is None


class TestFetchMarketOracle:
    """Perp market oracle reads layered on _fetch_account_data."""

    def _sdk(self, account_data: bytes | None) -> DriftSDK:
        sdk, _ = _gateway_sdk(_gateway_response())
        patch.object(sdk, "_fetch_account_data", return_value=account_data).start()
        return sdk

    def teardown_method(self):
        patch.stopall()

    def test_no_transport_returns_none(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        assert sdk.fetch_market_oracle(0) is None

    def test_missing_account_raises_market_error(self):
        sdk = self._sdk(None)
        with pytest.raises(DriftMarketError, match="not found"):
            sdk.fetch_market_oracle(7)

    def test_short_account_data_raises_market_error(self):
        sdk = self._sdk(bytes(PERP_MARKET_ORACLE_OFFSET + 31))
        with pytest.raises(DriftMarketError, match="too short"):
            sdk.fetch_market_oracle(7)

    def test_reads_oracle_at_fixed_offset(self):
        data = bytes(PERP_MARKET_ORACLE_OFFSET) + bytes(ORACLE) + b"\xff" * 8
        sdk = self._sdk(data)
        assert sdk.fetch_market_oracle(7) == ORACLE


class TestFetchSpotMarketOracle:
    """Spot market oracle reads — soft-fail (None) instead of raising."""

    def _sdk(self, account_data: bytes | None) -> DriftSDK:
        sdk, _ = _gateway_sdk(_gateway_response())
        patch.object(sdk, "_fetch_account_data", return_value=account_data).start()
        return sdk

    def teardown_method(self):
        patch.stopall()

    def test_no_transport_returns_none(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        assert sdk.fetch_spot_market_oracle(0) is None

    def test_missing_account_returns_none(self):
        assert self._sdk(None).fetch_spot_market_oracle(1) is None

    def test_short_account_data_returns_none(self):
        sdk = self._sdk(bytes(SPOT_MARKET_ORACLE_OFFSET + 31))
        assert sdk.fetch_spot_market_oracle(1) is None

    def test_reads_oracle_at_fixed_offset(self):
        data = bytes(SPOT_MARKET_ORACLE_OFFSET) + bytes(ORACLE)
        sdk = self._sdk(data)
        assert sdk.fetch_spot_market_oracle(1) == ORACLE


class TestFetchUserAccountParsing:
    """fetch_user_account end-to-end through the gateway transport."""

    def test_full_account_parses_positions_via_gateway(self):
        buf = _user_account_buffer(
            perp_base_amount=5_000_000,
            perp_market_index=4,
            spot_scaled_balance=9_999,
            spot_market_index=1,
        )
        sdk, _ = _gateway_sdk(_gateway_response(result=_account_info_result([_b64(bytes(buf)), "base64"])))

        account = sdk.fetch_user_account(sub_account_id=2)

        assert account.exists is True
        assert account.authority == TEST_WALLET
        assert account.sub_account_id == 2
        assert len(account.perp_positions) == MAX_PERP_POSITIONS
        assert len(account.spot_positions) == MAX_SPOT_POSITIONS
        assert account.active_perp_market_indexes == [4]
        assert account.active_spot_market_indexes == [1]
        pos = account.perp_positions[0]
        assert pos.base_asset_amount == 5_000_000
        assert pos.quote_asset_amount == -1_000_000
        assert pos.last_cumulative_funding_rate == 42
        assert pos.open_orders == 3
        spot = account.spot_positions[0]
        assert spot.scaled_balance == 9_999
        assert spot.balance_type == 1
        assert spot.open_orders == 2

    def test_authority_mismatch_logs_warning_but_parses(self, caplog):
        sdk, _ = _gateway_sdk(_gateway_response())
        buf = _user_account_buffer(authority=OTHER_WALLET)
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.drift.sdk"):
            account = sdk._parse_user_account(bytes(buf), 0)
        assert account.exists is True
        assert "authority mismatch" in caplog.text

    def test_authority_parse_failure_logs_warning(self, caplog):
        sdk, _ = _gateway_sdk(_gateway_response())
        buf = bytes(_user_account_buffer())
        with patch("almanak.connectors.drift.sdk.Pubkey") as pubkey_cls:
            pubkey_cls.from_bytes.side_effect = ValueError("bad bytes")
            with caplog.at_level(logging.WARNING, logger="almanak.connectors.drift.sdk"):
                account = sdk._parse_user_account(buf, 0)
        assert account.exists is True
        assert "Failed to parse authority" in caplog.text

    def test_data_shorter_than_authority_field_skips_check(self):
        sdk, _ = _gateway_sdk(_gateway_response())
        account = sdk._parse_user_account(bytes(USER_AUTHORITY_OFFSET + 31), 5)
        assert account.exists is True
        assert account.sub_account_id == 5
        assert account.perp_positions == []
        assert account.spot_positions == []

    def test_truncated_data_stops_at_last_complete_position(self):
        # Room for exactly one perp slot and zero spot slots.
        buf = bytes(_user_account_buffer(perp_base_amount=1))[: USER_PERP_POSITIONS_OFFSET + PERP_POSITION_SIZE]
        sdk, _ = _gateway_sdk(_gateway_response())
        account = sdk._parse_user_account(buf, 0)
        assert len(account.perp_positions) == 1
        assert account.spot_positions == []


class TestPositionParsersShortBuffers:
    """Optional trailing bytes default to 0 when the buffer is short."""

    def test_perp_position_without_open_orders_byte(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        data = bytearray(72)
        struct.pack_into("<q", data, 0, -7)
        struct.pack_into("<H", data, 24, 9)
        pos = sdk._parse_perp_position(bytes(data), 0)
        assert pos.base_asset_amount == -7
        assert pos.market_index == 9
        assert pos.open_orders == 0

    def test_spot_position_without_flag_bytes(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        data = bytearray(10)
        struct.pack_into("<Q", data, 0, 123)
        struct.pack_into("<H", data, 8, 6)
        pos = sdk._parse_spot_position(bytes(data), 0)
        assert pos.scaled_balance == 123
        assert pos.market_index == 6
        assert pos.balance_type == 0
        assert pos.open_orders == 0


class TestBuildRemainingAccounts:
    """Remaining-accounts assembly on top of the fetch helpers."""

    def test_no_transport_emits_markets_without_oracles(self):
        sdk = DriftSDK(wallet_address=TEST_WALLET)
        remaining = sdk.build_remaining_accounts(market_index=3)

        # No oracles resolvable offline: spot market 0 + traded perp market.
        assert len(remaining) == 2
        spot_meta, perp_meta = remaining
        assert spot_meta.pubkey == sdk.get_spot_market_pda(0)
        assert spot_meta.is_writable is False
        assert perp_meta.pubkey == sdk.get_perp_market_pda(3)
        assert perp_meta.is_writable is True
        assert all(meta.is_signer is False for meta in remaining)

    def test_existing_positions_included_and_oracles_deduped(self):
        sdk, _ = _gateway_sdk(_gateway_response())
        user = DriftUserAccount(
            authority=TEST_WALLET,
            perp_positions=[DriftPerpPosition(market_index=1, base_asset_amount=10)],
            spot_positions=[DriftSpotPosition(market_index=2, scaled_balance=5)],
            exists=True,
        )
        with (
            patch.object(sdk, "fetch_user_account", return_value=user),
            patch.object(sdk, "fetch_market_oracle", return_value=ORACLE),
            patch.object(sdk, "fetch_spot_market_oracle", return_value=ORACLE),
        ):
            remaining = sdk.build_remaining_accounts(market_index=3)

        # 1 deduped oracle + spot markets {0, 2} + perp markets {1, 3}.
        assert len(remaining) == 5
        assert remaining[0].pubkey == ORACLE
        assert remaining[0].is_writable is False
        assert [m.pubkey for m in remaining[1:3]] == [
            sdk.get_spot_market_pda(0),
            sdk.get_spot_market_pda(2),
        ]
        assert [m.pubkey for m in remaining[3:5]] == [
            sdk.get_perp_market_pda(1),
            sdk.get_perp_market_pda(3),
        ]
        assert all(m.is_writable for m in remaining[3:5])

    def test_nonexistent_user_account_adds_no_extra_markets(self):
        sdk, _ = _gateway_sdk(_gateway_response())
        with (
            patch.object(sdk, "fetch_user_account", return_value=DriftUserAccount(exists=False)),
            patch.object(sdk, "fetch_market_oracle", return_value=None),
            patch.object(sdk, "fetch_spot_market_oracle", return_value=None),
        ):
            remaining = sdk.build_remaining_accounts(market_index=3)

        # Only the defaults: spot market 0 + the traded perp market, no oracles.
        assert [m.pubkey for m in remaining] == [
            sdk.get_spot_market_pda(0),
            sdk.get_perp_market_pda(3),
        ]

    def test_oracle_fetch_failures_are_swallowed(self):
        sdk, _ = _gateway_sdk(_gateway_response())
        with patch.object(sdk, "fetch_market_oracle", side_effect=DriftMarketError("gone", market="3")):
            assert sdk._get_oracle_for_perp_market(3) is None
        with patch.object(
            sdk,
            "fetch_spot_market_oracle",
            side_effect=DriftMarketError("gone", market="0"),
        ):
            assert sdk._get_oracle_for_spot_market(0) is None
