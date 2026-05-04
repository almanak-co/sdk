"""Unit tests for Morpho Blue SDK.

These tests stub out RPC interactions via Web3 mocking to exercise the SDK's
on-chain reading code paths without any network access.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from web3.exceptions import ContractLogicError

from almanak.framework.connectors.morpho_blue.sdk import (
    LLTV_SCALE,
    MORPHO_DEPLOYMENT_BLOCKS,
    SUPPORTED_CHAINS,
    MarketNotFoundError,
    MorphoBlueSDK,
    MorphoBlueSDKError,
    RPCError,
    SDKMarketInfo,
    SDKMarketParams,
    SDKMarketState,
    SDKPosition,
    UnsupportedChainError,
)


def _make_sdk(chain: str = "ethereum") -> MorphoBlueSDK:
    """Build an SDK with all Web3 layers stubbed."""
    with patch("almanak.framework.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True
        # Web3() returns w3 instance; Web3.HTTPProvider is also looked up.
        # Web3.to_checksum_address is used for address normalisation.
        mock_web3_cls.return_value = mock_w3
        mock_web3_cls.to_checksum_address.side_effect = lambda x: x
        mock_web3_cls.HTTPProvider = MagicMock()
        with patch("almanak.framework.connectors.morpho_blue.sdk.get_rpc_url", return_value="http://x"):
            with patch("almanak.framework.connectors.morpho_blue.sdk.is_poa_chain", return_value=False):
                sdk = MorphoBlueSDK(chain=chain, rpc_url="http://test")
                # Replace the w3 with our deterministic mock
                sdk.w3 = mock_w3
                return sdk


# =============================================================================
# Initialization
# =============================================================================


class TestSDKInit:
    def test_unsupported_chain_raises(self) -> None:
        with pytest.raises(UnsupportedChainError):
            MorphoBlueSDK(chain="unknown_chain")

    def test_init_with_explicit_rpc(self) -> None:
        sdk = _make_sdk()
        assert sdk.chain == "ethereum"
        assert sdk.rpc_url == "http://test"

    def test_init_chain_normalized_to_lower(self) -> None:
        sdk = _make_sdk(chain="ETHEREUM")
        assert sdk.chain == "ethereum"

    def test_init_rpc_disconnected_raises(self) -> None:
        with patch("almanak.framework.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_w3.is_connected.return_value = False
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.to_checksum_address.side_effect = lambda x: x
            with patch("almanak.framework.connectors.morpho_blue.sdk.get_rpc_url", return_value="http://x"):
                with patch("almanak.framework.connectors.morpho_blue.sdk.is_poa_chain", return_value=False):
                    with pytest.raises(RPCError):
                        MorphoBlueSDK(chain="ethereum", rpc_url="http://test")

    def test_init_poa_chain_injects_middleware(self) -> None:
        with patch("almanak.framework.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_w3.is_connected.return_value = True
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.to_checksum_address.side_effect = lambda x: x
            with patch("almanak.framework.connectors.morpho_blue.sdk.get_rpc_url", return_value="http://x"):
                with patch("almanak.framework.connectors.morpho_blue.sdk.is_poa_chain", return_value=True):
                    sdk = MorphoBlueSDK(chain="ethereum", rpc_url="http://test")
                    assert sdk is not None
                    mock_w3.middleware_onion.inject.assert_called_once()

    def test_init_with_gateway_client(self) -> None:
        gateway_client = MagicMock()
        with patch("almanak.framework.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_w3.is_connected.return_value = True
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.to_checksum_address.side_effect = lambda x: x
            with patch("almanak.framework.connectors.morpho_blue.sdk.is_poa_chain", return_value=False):
                with patch(
                    "almanak.framework.web3.gateway_provider.GatewayWeb3Provider",
                    return_value=MagicMock(),
                ):
                    sdk = MorphoBlueSDK(chain="ethereum", gateway_client=gateway_client)
                    assert sdk._gateway_client is gateway_client


# =============================================================================
# Position Reading
# =============================================================================


class TestGetPosition:
    def test_decodes_position(self) -> None:
        sdk = _make_sdk()
        # Build mock result: 3 packed 32-byte values
        supply_shares = 100
        borrow_shares = 50
        collateral = 1
        encoded = (
            hex(supply_shares)[2:].zfill(64)
            + hex(borrow_shares)[2:].zfill(64)
            + hex(collateral)[2:].zfill(64)
        )
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        sdk.w3.eth.call.return_value = mock_result

        position = sdk.get_position("0x" + "ab" * 32, "0x" + "12" * 20)
        assert position.supply_shares == supply_shares
        assert position.borrow_shares == borrow_shares
        assert position.collateral == collateral

    def test_get_supply_shares(self) -> None:
        sdk = _make_sdk()
        encoded = hex(123)[2:].zfill(64) + hex(0)[2:].zfill(64) + hex(0)[2:].zfill(64)
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        sdk.w3.eth.call.return_value = mock_result
        assert sdk.get_supply_shares("0x" + "ab" * 32, "0x" + "12" * 20) == 123

    def test_get_borrow_shares(self) -> None:
        sdk = _make_sdk()
        encoded = hex(0)[2:].zfill(64) + hex(456)[2:].zfill(64) + hex(0)[2:].zfill(64)
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        sdk.w3.eth.call.return_value = mock_result
        assert sdk.get_borrow_shares("0x" + "ab" * 32, "0x" + "12" * 20) == 456

    def test_get_collateral(self) -> None:
        sdk = _make_sdk()
        encoded = hex(0)[2:].zfill(64) + hex(0)[2:].zfill(64) + hex(789)[2:].zfill(64)
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        sdk.w3.eth.call.return_value = mock_result
        assert sdk.get_collateral("0x" + "ab" * 32, "0x" + "12" * 20) == 789

    def test_get_position_contract_logic_error(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = ContractLogicError("revert")
        with pytest.raises(RPCError):
            sdk.get_position("0x" + "ab" * 32, "0x" + "12" * 20)

    def test_get_position_general_exception(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = RuntimeError("network down")
        with pytest.raises(RPCError):
            sdk.get_position("0x" + "ab" * 32, "0x" + "12" * 20)


# =============================================================================
# Market State Reading
# =============================================================================


class TestGetMarketState:
    def test_decodes_market_state(self) -> None:
        sdk = _make_sdk()
        # 6 uint128 fields packed into 3 uint256 words; each field is 32 hex chars
        encoded = (
            hex(1000)[2:].zfill(32)  # total_supply_assets (upper 128 of word 0)
            + hex(900)[2:].zfill(32)  # total_supply_shares (lower 128 of word 0)
            + hex(500)[2:].zfill(32)
            + hex(450)[2:].zfill(32)
            + hex(1234567890)[2:].zfill(32)
            + hex(0)[2:].zfill(32)
        )
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        sdk.w3.eth.call.return_value = mock_result

        state = sdk.get_market_state("0x" + "ab" * 32)
        assert state.total_supply_assets == 1000
        assert state.last_update == 1234567890

    def test_market_not_found_when_state_empty_and_no_params(self) -> None:
        sdk = _make_sdk()

        # All zeros = empty state; then get_market_params raises MarketNotFoundError
        empty = "0" * (32 * 6)
        params_result = "0" * 64 * 5  # zero loan_token triggers MarketNotFoundError

        mock_state_result = MagicMock()
        mock_state_result.hex.return_value = empty
        mock_params_result = MagicMock()
        mock_params_result.hex.return_value = params_result
        sdk.w3.eth.call.side_effect = [mock_state_result, mock_params_result]

        with pytest.raises(MarketNotFoundError):
            sdk.get_market_state("0x" + "ab" * 32)

    def test_market_state_contract_logic_error(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = ContractLogicError("revert")
        with pytest.raises(RPCError):
            sdk.get_market_state("0x" + "ab" * 32)

    def test_market_state_general_exception(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = RuntimeError("net err")
        with pytest.raises(RPCError):
            sdk.get_market_state("0x" + "ab" * 32)


# =============================================================================
# Market Params Reading
# =============================================================================


class TestGetMarketParams:
    def test_decodes_market_params(self) -> None:
        sdk = _make_sdk()
        loan = "0x" + "11" * 20
        coll = "0x" + "22" * 20
        oracle = "0x" + "33" * 20
        irm = "0x" + "44" * 20
        lltv = 860000000000000000

        # Each address is encoded as 64 hex chars (last 40 = address)
        encoded = (
            "0" * 24 + loan[2:]
            + "0" * 24 + coll[2:]
            + "0" * 24 + oracle[2:]
            + "0" * 24 + irm[2:]
            + hex(lltv)[2:].zfill(64)
        )
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded

        # Mock to_checksum_address called inside _decode_address
        with patch("almanak.framework.connectors.morpho_blue.sdk.Web3.to_checksum_address", side_effect=lambda x: x):
            sdk.w3.eth.call.return_value = mock_result
            p = sdk.get_market_params("0x" + "ab" * 32)
            assert p.lltv == lltv

    def test_market_not_found_when_loan_zero(self) -> None:
        sdk = _make_sdk()
        # All zeros
        encoded = "0" * 64 * 5
        mock_result = MagicMock()
        mock_result.hex.return_value = encoded
        with patch(
            "almanak.framework.connectors.morpho_blue.sdk.Web3.to_checksum_address",
            return_value="0x0000000000000000000000000000000000000000",
        ):
            sdk.w3.eth.call.return_value = mock_result
            with pytest.raises(MarketNotFoundError):
                sdk.get_market_params("0x" + "ab" * 32)

    def test_market_params_contract_logic_error(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = ContractLogicError("revert")
        with pytest.raises(RPCError):
            sdk.get_market_params("0x" + "ab" * 32)

    def test_market_params_general_exception(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.call.side_effect = RuntimeError("net err")
        with pytest.raises(RPCError):
            sdk.get_market_params("0x" + "ab" * 32)


# =============================================================================
# Get Market Info (combo)
# =============================================================================


class TestGetMarketInfo:
    def test_combines_params_and_state(self) -> None:
        sdk = _make_sdk()
        with patch.object(sdk, "get_market_params") as mock_params, patch.object(
            sdk, "get_market_state"
        ) as mock_state:
            mock_params.return_value = SDKMarketParams(
                market_id="0x1", loan_token="l", collateral_token="c", oracle="o", irm="i", lltv=1
            )
            mock_state.return_value = SDKMarketState(
                market_id="0x1",
                total_supply_assets=1,
                total_supply_shares=1,
                total_borrow_assets=0,
                total_borrow_shares=0,
                last_update=0,
                fee=0,
            )
            info = sdk.get_market_info("0x1")
            assert isinstance(info, SDKMarketInfo)


# =============================================================================
# Discover Markets
# =============================================================================


class TestDiscoverMarkets:
    def test_discover_markets_chunks_logs(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 100
        # One log with topic + market_id
        mock_topic = MagicMock()
        mock_topic.hex.return_value = "ab" * 32
        log = {"topics": [None, mock_topic]}
        sdk.w3.eth.get_logs.return_value = [log]

        markets = sdk.discover_markets(from_block=0, to_block=50, chunk_size=10)
        assert len(markets) > 0
        assert markets[0].startswith("0x")

    def test_discover_markets_uses_default_from_block(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 100
        sdk.w3.eth.get_logs.return_value = []

        markets = sdk.discover_markets(to_block=50, chunk_size=200)
        assert markets == []

    def test_discover_markets_latest_block(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 50
        sdk.w3.eth.get_logs.return_value = []
        markets = sdk.discover_markets(from_block=0, to_block="latest", chunk_size=200)
        assert markets == []

    def test_discover_markets_handles_logs_without_topics(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 50
        # Log with only one topic (no market_id) -> skipped
        sdk.w3.eth.get_logs.return_value = [{"topics": [None]}]
        markets = sdk.discover_markets(from_block=0, to_block=50, chunk_size=100)
        assert markets == []

    def test_discover_markets_exception(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 50
        sdk.w3.eth.get_logs.side_effect = RuntimeError("RPC down")
        with pytest.raises(RPCError):
            sdk.discover_markets(from_block=0, to_block=50)

    def test_get_market_count(self) -> None:
        sdk = _make_sdk()
        with patch.object(sdk, "discover_markets", return_value=["0x1", "0x2"]):
            assert sdk.get_market_count() == 2


# =============================================================================
# Utility
# =============================================================================


class TestUtilityMethods:
    def test_get_block_number(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.block_number = 42
        assert sdk.get_block_number() == 42

    def test_get_chain_id(self) -> None:
        sdk = _make_sdk()
        sdk.w3.eth.chain_id = 1
        assert sdk.get_chain_id() == 1

    def test_is_connected(self) -> None:
        sdk = _make_sdk()
        sdk.w3.is_connected.return_value = True
        assert sdk.is_connected()


class TestSharesAssetsConversion:
    def test_shares_to_assets(self) -> None:
        sdk = _make_sdk()
        assert sdk.shares_to_assets(100, 1000, 500) == 200

    def test_shares_to_assets_zero_total_shares(self) -> None:
        sdk = _make_sdk()
        assert sdk.shares_to_assets(100, 1000, 0) == 0

    def test_assets_to_shares(self) -> None:
        sdk = _make_sdk()
        assert sdk.assets_to_shares(200, 1000, 500) == 100

    def test_assets_to_shares_zero_total_assets(self) -> None:
        sdk = _make_sdk()
        assert sdk.assets_to_shares(200, 0, 500) == 0

    def test_get_supply_assets(self) -> None:
        sdk = _make_sdk()
        with patch.object(sdk, "get_position") as mock_pos, patch.object(
            sdk, "get_market_state"
        ) as mock_state:
            mock_pos.return_value = SDKPosition(
                market_id="0x1", user="0x2", supply_shares=100, borrow_shares=0, collateral=0
            )
            mock_state.return_value = SDKMarketState(
                market_id="0x1",
                total_supply_assets=2000,
                total_supply_shares=1000,
                total_borrow_assets=0,
                total_borrow_shares=0,
                last_update=0,
                fee=0,
            )
            assert sdk.get_supply_assets("0x1", "0x2") == 200

    def test_get_borrow_assets(self) -> None:
        sdk = _make_sdk()
        with patch.object(sdk, "get_position") as mock_pos, patch.object(
            sdk, "get_market_state"
        ) as mock_state:
            mock_pos.return_value = SDKPosition(
                market_id="0x1", user="0x2", supply_shares=0, borrow_shares=200, collateral=0
            )
            mock_state.return_value = SDKMarketState(
                market_id="0x1",
                total_supply_assets=0,
                total_supply_shares=0,
                total_borrow_assets=2000,
                total_borrow_shares=1000,
                last_update=0,
                fee=0,
            )
            assert sdk.get_borrow_assets("0x1", "0x2") == 400


class TestPrivateHelpers:
    def test_normalize_market_id_no_prefix(self) -> None:
        sdk = _make_sdk()
        norm = sdk._normalize_market_id("ab" * 32)
        assert norm.startswith("0x")
        assert len(norm) == 66

    def test_normalize_market_id_short(self) -> None:
        sdk = _make_sdk()
        norm = sdk._normalize_market_id("0x" + "ab" * 16)  # 32 hex chars (16 bytes)
        assert len(norm) == 66
        assert norm.startswith("0x" + "0" * 32)

    def test_normalize_market_id_already_correct(self) -> None:
        sdk = _make_sdk()
        market_id = "0x" + "ab" * 32
        assert sdk._normalize_market_id(market_id) == market_id

    def test_pad_address(self) -> None:
        sdk = _make_sdk()
        result = sdk._pad_address("0x12345")
        assert len(result) == 64
        assert result.endswith("12345")

    def test_decode_address(self) -> None:
        sdk = _make_sdk()
        with patch(
            "almanak.framework.connectors.morpho_blue.sdk.Web3.to_checksum_address",
            return_value="0xchecksum",
        ):
            result = sdk._decode_address("0" * 24 + "11" * 20)
            assert result == "0xchecksum"


# =============================================================================
# Data Class Tests
# =============================================================================


class TestSDKPosition:
    def test_has_methods_and_to_dict(self) -> None:
        p = SDKPosition(market_id="0x1", user="0x2", supply_shares=10, borrow_shares=5, collateral=1)
        assert p.has_supply
        assert p.has_borrow
        assert p.has_collateral
        assert not p.is_empty
        assert p.to_dict()["supply_shares"] == 10

    def test_empty_position(self) -> None:
        p = SDKPosition(market_id="0x1", user="0x2", supply_shares=0, borrow_shares=0, collateral=0)
        assert p.is_empty
        assert not p.has_supply
        assert not p.has_borrow


class TestSDKMarketState:
    def test_utilization(self) -> None:
        s = SDKMarketState(
            market_id="0x1",
            total_supply_assets=1000,
            total_supply_shares=900,
            total_borrow_assets=500,
            total_borrow_shares=450,
            last_update=1,
            fee=0,
        )
        assert s.utilization == Decimal("0.5")
        assert s.utilization_percent == Decimal("50")
        assert s.available_liquidity == 500
        assert s.fee_percent == Decimal("0")

    def test_utilization_zero_supply(self) -> None:
        s = SDKMarketState(
            market_id="0x1",
            total_supply_assets=0,
            total_supply_shares=0,
            total_borrow_assets=0,
            total_borrow_shares=0,
            last_update=0,
            fee=0,
        )
        assert s.utilization == Decimal("0")

    def test_to_dict(self) -> None:
        s = SDKMarketState(
            market_id="0x1",
            total_supply_assets=1000,
            total_supply_shares=900,
            total_borrow_assets=500,
            total_borrow_shares=450,
            last_update=1,
            fee=0,
        )
        d = s.to_dict()
        assert "utilization" in d


class TestSDKMarketParams:
    def test_lltv_props_and_to_dict(self) -> None:
        p = SDKMarketParams(
            market_id="0x1",
            loan_token="l",
            collateral_token="c",
            oracle="o",
            irm="i",
            lltv=860000000000000000,
        )
        assert p.lltv_percent == Decimal("86")
        assert p.lltv_decimal == Decimal("0.86")
        assert p.to_dict()["lltv"] == 860000000000000000


class TestSDKMarketInfo:
    def test_to_dict(self) -> None:
        params = SDKMarketParams(
            market_id="0x1", loan_token="l", collateral_token="c", oracle="o", irm="i", lltv=1
        )
        state = SDKMarketState(
            market_id="0x1",
            total_supply_assets=1,
            total_supply_shares=1,
            total_borrow_assets=0,
            total_borrow_shares=0,
            last_update=0,
            fee=0,
        )
        info = SDKMarketInfo(params=params, state=state)
        d = info.to_dict()
        assert "params" in d
        assert "state" in d


class TestExceptions:
    def test_sdk_error_base(self) -> None:
        err = MorphoBlueSDKError("base error")
        assert "base error" in str(err)


class TestConstants:
    def test_supported_chains_includes_arbitrum_polygon_monad(self) -> None:
        assert "arbitrum" in SUPPORTED_CHAINS
        assert "polygon" in SUPPORTED_CHAINS
        assert "monad" in SUPPORTED_CHAINS

    def test_lltv_scale(self) -> None:
        assert LLTV_SCALE == 10**18

    def test_deployment_blocks_present_for_supported_chains(self) -> None:
        for chain in SUPPORTED_CHAINS:
            assert chain in MORPHO_DEPLOYMENT_BLOCKS
