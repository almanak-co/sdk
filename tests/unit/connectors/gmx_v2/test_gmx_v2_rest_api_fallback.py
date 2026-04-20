"""Tests for GMX V2 REST API position query fallback.

Validates that GMXV2SDK falls back to the GMX REST API when all on-chain
Reader contract methods fail. Addresses VIB-1947.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestGMXV2RestApiFallback:
    """Tests for get_account_positions falling back to REST API."""

    MOCK_POSITION_RAW = (
        ("0xAccount", "0xMarket", "0xCollateral"),
        (1000, 500, 100, 0, 0, 0, 0, 1000, 0, 1700000000, 0),
        (True,),
    )

    def _make_sdk(self):
        """Create a GMXV2SDK with mocked internals."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = MagicMock()
        sdk.web3.to_checksum_address = lambda x: x
        sdk.chain = "arbitrum"
        sdk.reader = MagicMock()
        sdk.DATA_STORE_ADDRESS = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"
        sdk.READER_ADDRESS = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
        return sdk

    def test_reader_works_no_api_call(self):
        """When on-chain Reader works, REST API is not called."""
        sdk = self._make_sdk()
        sdk.reader.functions.getAccountPositionCount.return_value.call.return_value = 1
        sdk.reader.functions.getAccountPositions.return_value.call.return_value = [self.MOCK_POSITION_RAW]

        with patch.object(sdk, "_get_positions_via_api") as api_mock:
            with patch.object(sdk, "_get_position_count_from_datastore"):
                positions = sdk.get_account_positions("0x1234")

        assert len(positions) == 1
        api_mock.assert_not_called()

    def test_reader_fails_falls_back_to_api(self):
        """When all on-chain methods fail, falls back to REST API."""
        sdk = self._make_sdk()
        # Both count and range queries fail
        sdk.reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")
        sdk.reader.functions.getAccountPositions.return_value.call.side_effect = Exception("also reverted")

        mock_api_result = [{"account": "0x1234", "market": "0xMarket", "is_long": True, "size_in_usd": 5000}]

        with patch.object(sdk, "_get_position_count_from_datastore", side_effect=Exception("nope")):
            with patch.object(sdk, "_get_positions_via_api", return_value=mock_api_result) as api_mock:
                positions = sdk.get_account_positions("0x1234")

        assert positions == mock_api_result
        api_mock.assert_called_once_with("0x1234")

    def test_reader_returns_empty_no_api_fallback(self):
        """When Reader returns empty list (genuinely no positions), no API fallback."""
        sdk = self._make_sdk()
        sdk.reader.functions.getAccountPositionCount.return_value.call.return_value = 0
        sdk.reader.functions.getAccountPositions.return_value.call.return_value = []

        with patch.object(sdk, "_get_positions_via_api") as api_mock:
            with patch.object(sdk, "_get_position_count_from_datastore"):
                positions = sdk.get_account_positions("0x1234")

        assert positions == []
        api_mock.assert_not_called()


class TestGMXV2GetPositionsViaApi:
    """Tests for GMXV2SDK._get_positions_via_api."""

    def _make_sdk(self, chain="arbitrum"):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = MagicMock()
        sdk.chain = chain
        return sdk

    SAMPLE_API_RESPONSE = {
        "positions": [
            {
                "account": "0xABCD1234",
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateralToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "sizeInUsd": "4332882579080000000000000000000",
                "sizeInTokens": "2200000000000000",
                "collateralAmount": "4000000",
                "borrowingFactor": "12345",
                "fundingFeeAmountPerSize": "6789",
                "longTokenClaimableFundingAmountPerSize": "0",
                "shortTokenClaimableFundingAmountPerSize": "0",
                "increasedAtBlock": "200000000",
                "decreasedAtBlock": "0",
                "increasedAtTime": "1700000000",
                "decreasedAtTime": "0",
                "isLong": True,
            },
        ]
    }

    # Also test list-style response (API may return positions directly)
    SAMPLE_API_RESPONSE_LIST = [
        {
            "account": "0xABCD1234",
            "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            "collateralToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "sizeInUsd": "4332882579080000000000000000000",
            "sizeInTokens": "2200000000000000",
            "collateralAmount": "4000000",
            "borrowingFactor": "12345",
            "fundingFeeAmountPerSize": "6789",
            "longTokenClaimableFundingAmountPerSize": "0",
            "shortTokenClaimableFundingAmountPerSize": "0",
            "increasedAtBlock": "200000000",
            "decreasedAtBlock": "0",
            "increasedAtTime": "1700000000",
            "decreasedAtTime": "0",
            "isLong": True,
        },
    ]

    def test_api_returns_positions_dict_format(self):
        """REST API response in dict format with 'positions' key is parsed correctly."""
        sdk = self._make_sdk()

        response_bytes = json.dumps(self.SAMPLE_API_RESPONSE).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = response_bytes
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen", return_value=mock_resp):
            positions = sdk._get_positions_via_api("0xabcd1234")

        assert len(positions) == 1
        assert positions[0]["account"] == "0xABCD1234"
        assert positions[0]["size_in_usd"] == 4332882579080000000000000000000
        assert positions[0]["collateral_token"] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert positions[0]["is_long"] is True
        assert positions[0]["increased_at_time"] == 1700000000
        assert positions[0]["borrowing_factor"] == 12345

    def test_api_returns_positions_list_format(self):
        """REST API response in list format (direct array) is parsed correctly."""
        sdk = self._make_sdk()

        response_bytes = json.dumps(self.SAMPLE_API_RESPONSE_LIST).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = response_bytes
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen", return_value=mock_resp):
            positions = sdk._get_positions_via_api("0xabcd1234")

        assert len(positions) == 1
        assert positions[0]["size_in_usd"] == 4332882579080000000000000000000

    def test_api_empty_positions(self):
        """REST API returns empty positions list."""
        sdk = self._make_sdk()

        response_bytes = json.dumps({"positions": []}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = response_bytes
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen", return_value=mock_resp):
            positions = sdk._get_positions_via_api("0xNonExistent")

        assert positions == []

    def test_api_request_timeout(self):
        """REST API timeout raises PositionQueryError."""
        from almanak.framework.connectors.gmx_v2.sdk import PositionQueryError

        sdk = self._make_sdk()

        with patch(
            "almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen",
            side_effect=TimeoutError("timed out"),
        ):
            with pytest.raises(PositionQueryError, match="failed"):
                sdk._get_positions_via_api("0x1234")

    def test_api_http_error(self):
        """REST API HTTP error raises PositionQueryError."""
        import urllib.error

        from almanak.framework.connectors.gmx_v2.sdk import PositionQueryError

        sdk = self._make_sdk()

        with patch(
            "almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen",
            side_effect=urllib.error.URLError("Connection refused"),
        ):
            with pytest.raises(PositionQueryError, match="failed"):
                sdk._get_positions_via_api("0x1234")

    def test_api_invalid_json(self):
        """REST API returns invalid JSON — raises PositionQueryError."""
        from almanak.framework.connectors.gmx_v2.sdk import PositionQueryError

        sdk = self._make_sdk()

        mock_resp = MagicMock()
        mock_resp.read.return_value = b"not json"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("almanak.framework.connectors.gmx_v2.sdk.urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(PositionQueryError, match="failed"):
                sdk._get_positions_via_api("0x1234")

    def test_api_unsupported_chain(self):
        """Unsupported chain raises PositionQueryError."""
        from almanak.framework.connectors.gmx_v2.sdk import PositionQueryError

        sdk = self._make_sdk(chain="base")

        with pytest.raises(PositionQueryError, match="No GMX REST API URL configured"):
            sdk._get_positions_via_api("0x1234")


class TestGMXV2ParseApiPositions:
    """Tests for GMXV2SDK._parse_api_positions."""

    def test_parse_full_position(self):
        """Parse a complete API position with all fields."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        api_pos = {
            "account": "0xWallet",
            "market": "0xMarket",
            "collateralToken": "0xUSDC",
            "sizeInUsd": "5000000000000000000000000000000",
            "sizeInTokens": "2500000000000000000",
            "collateralAmount": "5000000",
            "borrowingFactor": "100",
            "fundingFeeAmountPerSize": "200",
            "longTokenClaimableFundingAmountPerSize": "0",
            "shortTokenClaimableFundingAmountPerSize": "0",
            "increasedAtBlock": "12345",
            "decreasedAtBlock": "0",
            "increasedAtTime": "1700000000",
            "decreasedAtTime": "0",
            "isLong": True,
        }

        positions = GMXV2SDK._parse_api_positions([api_pos])
        assert len(positions) == 1
        p = positions[0]
        assert p["account"] == "0xWallet"
        assert p["market"] == "0xMarket"
        assert p["collateral_token"] == "0xUSDC"
        assert p["size_in_usd"] == 5000000000000000000000000000000
        assert p["size_in_tokens"] == 2500000000000000000
        assert p["collateral_amount"] == 5000000
        assert p["is_long"] is True

    def test_parse_missing_fields_default_zero(self):
        """Missing numeric fields default to 0."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        api_pos = {"account": "0xWallet", "market": "0xMarket", "isLong": False}

        positions = GMXV2SDK._parse_api_positions([api_pos])
        assert len(positions) == 1
        p = positions[0]
        assert p["size_in_usd"] == 0
        assert p["collateral_amount"] == 0
        assert p["is_long"] is False
        assert p["collateral_token"] == ""

    def test_parse_multiple_positions(self):
        """Parse multiple positions."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        api_positions = [
            {"account": "0xA", "market": "0xM1", "sizeInUsd": "1000", "isLong": True},
            {"account": "0xA", "market": "0xM2", "sizeInUsd": "2000", "isLong": False},
        ]

        positions = GMXV2SDK._parse_api_positions(api_positions)
        assert len(positions) == 2
        assert positions[0]["size_in_usd"] == 1000
        assert positions[1]["size_in_usd"] == 2000


class TestGMXV2SafeInt:
    """Tests for GMXV2SDK._safe_int robustness."""

    def test_safe_int_string(self):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        assert GMXV2SDK._safe_int("12345", "test") == 12345

    def test_safe_int_scientific_notation(self):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        assert GMXV2SDK._safe_int("1.5e10", "test") == 15000000000

    def test_safe_int_none(self):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        assert GMXV2SDK._safe_int(None, "test") == 0

    def test_safe_int_invalid_string(self):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        assert GMXV2SDK._safe_int("not_a_number", "test") == 0

    def test_safe_int_integer_passthrough(self):
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        assert GMXV2SDK._safe_int(42, "test") == 42

    def test_parse_skips_malformed_position(self):
        """Malformed position is skipped, valid ones still parsed."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        api_positions = [
            {"account": "0xA", "market": "0xM1", "sizeInUsd": "1000", "isLong": True},
            {"account": "0xA", "market": "0xM2", "sizeInUsd": "bad_value", "isLong": True},
        ]

        positions = GMXV2SDK._parse_api_positions(api_positions)
        # Both should parse — _safe_int handles bad values gracefully
        assert len(positions) == 2
        assert positions[0]["size_in_usd"] == 1000
        assert positions[1]["size_in_usd"] == 0  # Defaults to 0 for unparseable value
