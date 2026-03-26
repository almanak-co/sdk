"""Tests for GMX V2 position query resilience.

Validates that SDK position queries handle Reader contract reverts gracefully
with fallback mechanisms (DataStore count, direct range queries).

Addresses VIB-1887: GMX V2 Reader contract calls revert.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestGMXV2SDKPositionCount:
    """Tests for GMXV2SDK.get_account_position_count with fallbacks."""

    def _make_sdk(self, reader_mock=None, web3_mock=None):
        """Create a GMXV2SDK with mocked Web3 and contracts."""
        with patch("almanak.framework.connectors.gmx_v2.sdk.Web3") as MockWeb3:
            mock_w3 = MagicMock()
            mock_w3.to_checksum_address = lambda x: x
            MockWeb3.return_value = mock_w3
            MockWeb3.HTTPProvider = MagicMock()
            MockWeb3.keccak = MagicMock(return_value=b"\x00" * 32)

            with patch("almanak.framework.connectors.gmx_v2.sdk.GMXV2SDK._load_abi", return_value=[]):
                with patch("almanak.framework.connectors.gmx_v2.sdk.GMXV2SDK._load_contract") as mock_load:
                    reader = reader_mock or MagicMock()
                    mock_load.return_value = reader

                    from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

                    sdk = GMXV2SDK.__new__(GMXV2SDK)
                    sdk.web3 = mock_w3
                    sdk.chain = "arbitrum"
                    sdk.reader = reader
                    sdk.DATA_STORE_ADDRESS = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"
                    sdk.READER_ADDRESS = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
                    return sdk

    def test_count_success(self):
        """Reader.getAccountPositionCount works — returns count directly."""
        reader = MagicMock()
        reader.functions.getAccountPositionCount.return_value.call.return_value = 3
        sdk = self._make_sdk(reader_mock=reader)

        count = sdk.get_account_position_count("0x1234")
        assert count == 3

    def test_count_reader_reverts_fallback_datastore(self):
        """Reader reverts → falls back to DataStore.getBytes32Count."""
        reader = MagicMock()
        reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("execution reverted")
        sdk = self._make_sdk(reader_mock=reader)

        with patch.object(sdk, "_get_position_count_from_datastore", return_value=2) as ds_mock:
            count = sdk.get_account_position_count("0x1234")
            assert count == 2
            ds_mock.assert_called_once()

    def test_count_both_fail_returns_zero(self):
        """Both Reader and DataStore fail → returns 0 (does not raise)."""
        reader = MagicMock()
        reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")
        sdk = self._make_sdk(reader_mock=reader)

        with patch.object(sdk, "_get_position_count_from_datastore", side_effect=Exception("also reverted")):
            count = sdk.get_account_position_count("0x1234")
            assert count == 0


class TestGMXV2SDKGetPositions:
    """Tests for GMXV2SDK.get_account_positions resilient flow."""

    MOCK_POSITION_RAW = (
        ("0xAccount", "0xMarket", "0xCollateral"),  # addresses
        (1000, 500, 100, 0, 0, 0, 0, 1000, 0, 1700000000, 0),  # numbers
        (True,),  # flags
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

    def test_positions_with_exact_count(self):
        """Normal flow: count=1, fetches exact range."""
        sdk = self._make_sdk()
        sdk.reader.functions.getAccountPositionCount.return_value.call.return_value = 1
        sdk.reader.functions.getAccountPositions.return_value.call.return_value = [self.MOCK_POSITION_RAW]

        with patch.object(sdk, "_get_position_count_from_datastore"):
            positions = sdk.get_account_positions("0x1234")

        assert len(positions) == 1
        assert positions[0]["market"] == "0xMarket"
        assert positions[0]["is_long"] is True
        assert positions[0]["size_in_usd"] == 1000

    def test_positions_count_zero_range_fallback_finds_positions(self):
        """Count returns 0 but range query finds positions (count method broken)."""
        sdk = self._make_sdk()
        # Count returns 0 (broken)
        sdk.reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")

        # Range query finds a position
        sdk.reader.functions.getAccountPositions.return_value.call.return_value = [self.MOCK_POSITION_RAW]

        with patch.object(sdk, "_get_position_count_from_datastore", side_effect=Exception("also broken")):
            positions = sdk.get_account_positions("0x1234")

        assert len(positions) == 1
        assert positions[0]["market"] == "0xMarket"

    def test_positions_everything_reverts_returns_empty(self):
        """All queries revert → returns empty list, no crash."""
        sdk = self._make_sdk()
        sdk.reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")
        sdk.reader.functions.getAccountPositions.return_value.call.side_effect = Exception("also reverted")

        with patch.object(sdk, "_get_position_count_from_datastore", side_effect=Exception("nope")):
            positions = sdk.get_account_positions("0x1234")

        assert positions == []

    def test_positions_empty_when_genuinely_no_positions(self):
        """Account has zero positions — returns empty list."""
        sdk = self._make_sdk()
        sdk.reader.functions.getAccountPositionCount.return_value.call.return_value = 0
        sdk.reader.functions.getAccountPositions.return_value.call.return_value = []

        with patch.object(sdk, "_get_position_count_from_datastore"):
            positions = sdk.get_account_positions("0x1234")

        assert positions == []


class TestGMXV2SDKParsePositions:
    """Tests for GMXV2SDK._parse_raw_positions static method."""

    def test_parse_raw_positions(self):
        """Parse raw Reader tuples into position dicts."""
        from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

        raw = [
            (
                ("0xAcc", "0xMkt", "0xCol"),
                (10**30, 5 * 10**17, 10**6, 100, 200, 300, 400, 100, 0, 1700000000, 0),
                (False,),
            )
        ]
        positions = GMXV2SDK._parse_raw_positions(raw)
        assert len(positions) == 1
        p = positions[0]
        assert p["account"] == "0xAcc"
        assert p["market"] == "0xMkt"
        assert p["collateral_token"] == "0xCol"
        assert p["size_in_usd"] == 10**30
        assert p["is_long"] is False
        assert p["increased_at_time"] == 1700000000


class TestGMXv2AdapterPositionsOnchain:
    """Tests for adapter.get_positions_onchain resilience."""

    def test_adapter_arbitrum_delegates_to_sdk(self):
        """Arbitrum path delegates to GMXV2SDK.get_account_positions."""
        from almanak.framework.connectors.gmx_v2.adapter import GMXv2Adapter

        adapter = GMXv2Adapter.__new__(GMXv2Adapter)
        adapter.chain = "arbitrum"
        adapter.wallet_address = "0x1234"
        adapter.addresses = {
            "synthetics_reader": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789",
            "data_store": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
        }

        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = []

        with patch("almanak.framework.connectors.gmx_v2.sdk.GMXV2SDK", return_value=mock_sdk):
            positions = adapter.get_positions_onchain("http://rpc")

        assert positions == []
        mock_sdk.get_account_positions.assert_called_once_with("0x1234")

    def test_adapter_unsupported_chain_raises(self):
        """Non-Arbitrum/Avalanche chain raises ValueError."""
        from almanak.framework.connectors.gmx_v2.adapter import GMXv2Adapter

        adapter = GMXv2Adapter.__new__(GMXv2Adapter)
        adapter.chain = "base"

        with pytest.raises(ValueError, match="not supported"):
            adapter.get_positions_onchain("http://rpc")

    def test_adapter_missing_addresses_raises(self):
        """Missing reader or data_store raises ValueError."""
        from almanak.framework.connectors.gmx_v2.adapter import GMXv2Adapter

        adapter = GMXv2Adapter.__new__(GMXv2Adapter)
        adapter.chain = "arbitrum"
        adapter.addresses = {}

        with pytest.raises(ValueError, match="Missing reader"):
            adapter.get_positions_onchain("http://rpc")


class TestGMXv2AdapterNonArbitrumPath:
    """Tests for adapter.get_positions_onchain non-Arbitrum (direct Web3) path."""

    MOCK_RAW_POSITION = (
        ("0xAccount", "0xMarket", "0xCollateral"),
        (1000, 500, 100, 0, 0, 0, 0, 1000, 0, 1700000000, 0),
        (True,),
    )

    def _make_avalanche_adapter(self):
        from almanak.framework.connectors.gmx_v2.adapter import GMXv2Adapter

        adapter = GMXv2Adapter.__new__(GMXv2Adapter)
        adapter.chain = "avalanche"
        adapter.wallet_address = "0xWallet"
        adapter.addresses = {
            "synthetics_reader": "0xReader",
            "data_store": "0xDataStore",
        }
        return adapter

    def _run_with_mocked_web3(self, adapter, mock_reader):
        """Run get_positions_onchain with a mocked Web3 and reader contract."""
        mock_w3 = MagicMock()
        mock_w3.to_checksum_address = lambda x: x
        mock_w3.eth.contract.return_value = mock_reader

        with patch("web3.Web3", return_value=mock_w3) as MockWeb3:
            MockWeb3.HTTPProvider = MagicMock()
            MockWeb3.return_value = mock_w3
            # Mock _parse_raw_positions to avoid needing full adapter context
            # (token resolver, markets, etc.) — we're testing query/fallback logic
            with patch.object(adapter, "_parse_raw_positions", side_effect=lambda rp: [{"mock": True}] * len(rp)):
                with patch("builtins.open", MagicMock()):
                    with patch("json.load", return_value=[]):
                        return adapter.get_positions_onchain("http://rpc")

    def test_non_arb_count_positive_returns_positions(self):
        """Non-Arbitrum: count > 0, fetches exact range."""
        adapter = self._make_avalanche_adapter()
        mock_reader = MagicMock()
        mock_reader.functions.getAccountPositionCount.return_value.call.return_value = 1
        mock_reader.functions.getAccountPositions.return_value.call.return_value = [self.MOCK_RAW_POSITION]

        positions = self._run_with_mocked_web3(adapter, mock_reader)
        assert len(positions) == 1

    def test_non_arb_count_reverts_range_fallback(self):
        """Non-Arbitrum: count reverts, falls back to range query."""
        adapter = self._make_avalanche_adapter()
        mock_reader = MagicMock()
        mock_reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")
        mock_reader.functions.getAccountPositions.return_value.call.return_value = [self.MOCK_RAW_POSITION]

        positions = self._run_with_mocked_web3(adapter, mock_reader)
        assert len(positions) == 1

    def test_non_arb_everything_reverts_returns_empty(self):
        """Non-Arbitrum: both count and range revert, returns []."""
        adapter = self._make_avalanche_adapter()
        mock_reader = MagicMock()
        mock_reader.functions.getAccountPositionCount.return_value.call.side_effect = Exception("reverted")
        mock_reader.functions.getAccountPositions.return_value.call.side_effect = Exception("also reverted")

        positions = self._run_with_mocked_web3(adapter, mock_reader)
        assert positions == []

    def test_non_arb_exact_range_reverts_returns_empty(self):
        """Non-Arbitrum: count succeeds but getAccountPositions reverts, returns []."""
        adapter = self._make_avalanche_adapter()
        mock_reader = MagicMock()
        mock_reader.functions.getAccountPositionCount.return_value.call.return_value = 2
        mock_reader.functions.getAccountPositions.return_value.call.side_effect = Exception("reverted")

        positions = self._run_with_mocked_web3(adapter, mock_reader)
        assert positions == []
