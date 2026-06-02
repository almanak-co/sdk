"""Unit tests for the amount_resolver module.

Tests the resolve_amount_all() function and ProtocolBalanceReader implementations
without requiring on-chain execution or gateway connectivity.
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.amount_resolver import (
    _INTENT_TYPE_TO_CATEGORY,
    AaveV3BalanceReader,
    AmountResolutionCategory,
    CompoundV3BalanceReader,
    MorphoBlueBalanceReader,
    _get_token_decimals,
    _resolve_token_address,
    _set_repay_full,
    _set_resolved_amount,
    _set_withdraw_all,
    get_reader_for_protocol,
    resolve_amount_all,
)

# =============================================================================
# Semantic Category Mapping
# =============================================================================


class TestSemanticCategories:
    """Test that intent types map to correct resolution categories."""

    def test_withdraw_maps_to_protocol_supply(self):
        assert _INTENT_TYPE_TO_CATEGORY["WITHDRAW"] == AmountResolutionCategory.PROTOCOL_SUPPLY

    def test_repay_maps_to_protocol_debt(self):
        assert _INTENT_TYPE_TO_CATEGORY["REPAY"] == AmountResolutionCategory.PROTOCOL_DEBT

    def test_swap_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["SWAP"] == AmountResolutionCategory.WALLET_BALANCE

    def test_supply_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["SUPPLY"] == AmountResolutionCategory.WALLET_BALANCE

    def test_bridge_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["BRIDGE"] == AmountResolutionCategory.WALLET_BALANCE


# =============================================================================
# Reader Registry
# =============================================================================


class TestReaderRegistry:
    """Test protocol-to-reader lookups."""

    def test_aave_v3_reader(self):
        reader = get_reader_for_protocol("aave_v3")
        assert isinstance(reader, AaveV3BalanceReader)

    def test_spark_reader(self):
        reader = get_reader_for_protocol("spark")
        assert isinstance(reader, AaveV3BalanceReader)

    def test_compound_v3_reader(self):
        reader = get_reader_for_protocol("compound_v3")
        assert isinstance(reader, CompoundV3BalanceReader)

    def test_morpho_reader(self):
        reader = get_reader_for_protocol("morpho_blue")
        assert isinstance(reader, MorphoBlueBalanceReader)

    def test_unknown_protocol_returns_none(self):
        assert get_reader_for_protocol("unknown_protocol") is None


# =============================================================================
# resolve_amount_all() — passthrough cases
# =============================================================================


class TestResolveAmountAllPassthrough:
    """Test that intents without amount='all' pass through unchanged."""

    def test_concrete_amount_passes_through(self):
        intent = MagicMock()
        intent.amount = Decimal("100")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_withdraw_all_flag_passes_through(self):
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = True
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_repay_full_flag_passes_through(self):
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = False
        intent.repay_full = True
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_no_intent_type_passes_through(self):
        intent = MagicMock(spec=[])  # No attributes
        intent.amount = "all"
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent


# =============================================================================
# resolve_amount_all() — withdraw resolution
# =============================================================================


class TestResolveAmountAllWithdraw:
    """Test withdraw amount='all' resolution paths."""

    def _make_withdraw_intent(self, protocol="aave_v3", token="USDC"):
        """Create a mock WithdrawIntent."""
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def test_withdraw_unknown_protocol_sets_withdraw_all(self):
        """Unknown protocol should fall back to withdraw_all=True."""
        intent = self._make_withdraw_intent(protocol="unknown_lending")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.withdraw_all is True
        assert result.amount == Decimal("0")

    def test_withdraw_aave_no_gateway_sets_withdraw_all(self):
        """Aave V3 without gateway client should fall back to withdraw_all=True."""
        intent = self._make_withdraw_intent(protocol="aave_v3")
        # No gateway_client -> LendingPositionReader returns None -> withdraw_all
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.withdraw_all is True

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    @patch("almanak.framework.intents.amount_resolver._get_token_decimals")
    def test_withdraw_aave_resolves_concrete_amount(
        self, mock_decimals, mock_resolve, mock_supply
    ):
        """Aave V3 with successful balance query resolves to concrete amount."""
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = 100_000_000  # 100 USDC in wei (6 decimals)
        mock_decimals.return_value = 6

        intent = self._make_withdraw_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.amount == Decimal("100")
        assert result.withdraw_all is False

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_withdraw_zero_balance_sets_withdraw_all(self, mock_resolve, mock_supply):
        """Zero balance should fall back to withdraw_all=True (nothing to withdraw)."""
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = 0

        intent = self._make_withdraw_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True


# =============================================================================
# resolve_amount_all() — repay resolution
# =============================================================================


class TestResolveAmountAllRepay:
    """Test repay amount='all' resolution paths."""

    def _make_repay_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def test_repay_unknown_protocol_sets_repay_full(self):
        intent = self._make_repay_intent(protocol="unknown_lending")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.repay_full is True
        assert result.amount == Decimal("0")

    def test_repay_aave_no_gateway_sets_repay_full(self):
        intent = self._make_repay_intent(protocol="aave_v3")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.repay_full is True


# =============================================================================
# Wallet balance intents (swap, supply) — passthrough
# =============================================================================


class TestWalletBalancePassthrough:
    """Test that wallet-balance intents pass through (resolved by caller)."""

    def test_swap_amount_all_passes_through(self):
        """SwapIntent(amount='all') should pass through — resolved by compiler/runner."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
            chain="arbitrum",
        )
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.amount == "all"  # Unchanged — wallet balance resolved by caller


# =============================================================================
# ProtocolBalanceReader implementations
# =============================================================================


class TestMorphoBlueBalanceReader:
    """Test Morpho Blue reader returns None (delegates to adapter)."""

    def test_supply_returns_none(self):
        reader = MorphoBlueBalanceReader()
        result = reader.get_supply_balance("ethereum", "0x1234", "0x5678")
        assert result is None

    def test_debt_returns_none(self):
        reader = MorphoBlueBalanceReader()
        result = reader.get_debt_balance("ethereum", "0x1234", "0x5678")
        assert result is None


class TestCompoundV3BalanceReader:
    """Test Compound V3 reader."""

    def test_no_gateway_returns_none(self):
        reader = CompoundV3BalanceReader()
        result = reader.get_supply_balance("arbitrum", "0x1234", "0x5678", gateway_client=None)
        assert result is None

    def test_unknown_chain_returns_none(self):
        reader = CompoundV3BalanceReader()
        result = reader.get_supply_balance(
            "unknown_chain", "0x1234", "0x5678", gateway_client=MagicMock()
        )
        assert result is None


# =============================================================================
# AaveV3BalanceReader - position-backed branches
# =============================================================================


class TestAaveV3BalanceReaderWithPosition:
    """Exercise the return-branches of AaveV3BalanceReader when a position exists."""

    @patch("almanak.framework.valuation.lending_position_reader.LendingPositionReader")
    def test_supply_returns_atoken_balance(self, mock_reader_cls):
        mock_reader = MagicMock()
        mock_reader.read_position.return_value = SimpleNamespace(
            current_atoken_balance=123_456_789,
            total_debt=0,
        )
        mock_reader_cls.return_value = mock_reader

        reader = AaveV3BalanceReader()
        result = reader.get_supply_balance(
            "arbitrum", "0xToken", "0xWallet", gateway_client=MagicMock()
        )
        assert result == 123_456_789

    @patch("almanak.framework.valuation.lending_position_reader.LendingPositionReader")
    def test_debt_returns_total_debt(self, mock_reader_cls):
        mock_reader = MagicMock()
        mock_reader.read_position.return_value = SimpleNamespace(
            current_atoken_balance=0,
            total_debt=987_654_321,
        )
        mock_reader_cls.return_value = mock_reader

        reader = AaveV3BalanceReader()
        result = reader.get_debt_balance(
            "arbitrum", "0xToken", "0xWallet", gateway_client=MagicMock()
        )
        assert result == 987_654_321

    @patch("almanak.framework.valuation.lending_position_reader.LendingPositionReader")
    def test_supply_returns_none_when_position_missing(self, mock_reader_cls):
        mock_reader = MagicMock()
        mock_reader.read_position.return_value = None
        mock_reader_cls.return_value = mock_reader

        reader = AaveV3BalanceReader()
        result = reader.get_supply_balance(
            "arbitrum", "0xToken", "0xWallet", gateway_client=MagicMock()
        )
        assert result is None

    @patch("almanak.framework.valuation.lending_position_reader.LendingPositionReader")
    def test_debt_returns_none_when_position_missing(self, mock_reader_cls):
        mock_reader = MagicMock()
        mock_reader.read_position.return_value = None
        mock_reader_cls.return_value = mock_reader

        reader = AaveV3BalanceReader()
        result = reader.get_debt_balance(
            "arbitrum", "0xToken", "0xWallet", gateway_client=MagicMock()
        )
        assert result is None


# =============================================================================
# CompoundV3BalanceReader - comet address lookups and eth_call paths
# =============================================================================


_COMET_ADDRESSES_PATCH_TARGET = "almanak.connectors.compound_v3.adapter.COMPOUND_V3_COMET_ADDRESSES"

_SINGLE = "0x" + "1" * 40
_MULTI_A = "0x" + "a" * 40
_MULTI_B = "0x" + "b" * 40


class TestCompoundV3CometAddress:
    """Test the internal _get_comet_address helper.

    These tests patch COMPOUND_V3_COMET_ADDRESSES to synthetic data so that
    the resolver logic (not the live address table) is what gets exercised.
    """

    def test_chain_with_single_market_returns_that_market(self):
        """When exactly one market exists on the chain and no market_id is given,
        the reader may safely use the sole market."""
        reader = CompoundV3BalanceReader()
        with patch.dict(
            _COMET_ADDRESSES_PATCH_TARGET,
            {"testchain": {"usdc": _SINGLE}},
            clear=True,
        ):
            assert reader._get_comet_address("testchain", market_id=None) == _SINGLE

    def test_chain_with_multiple_markets_and_no_market_id_returns_none(self):
        """Ambiguous market resolution must return None (not guess)."""
        reader = CompoundV3BalanceReader()
        with patch.dict(
            _COMET_ADDRESSES_PATCH_TARGET,
            {"testchain": {"usdc": _MULTI_A, "weth": _MULTI_B}},
            clear=True,
        ):
            assert reader._get_comet_address("testchain", market_id=None) is None

    def test_unknown_chain_with_no_market_id_returns_none(self):
        reader = CompoundV3BalanceReader()
        with patch.dict(_COMET_ADDRESSES_PATCH_TARGET, {}, clear=True):
            assert reader._get_comet_address("doesnt_exist", market_id=None) is None

    def test_known_chain_with_known_market_id_returns_address(self):
        reader = CompoundV3BalanceReader()
        with patch.dict(
            _COMET_ADDRESSES_PATCH_TARGET,
            {"testchain": {"usdc": _SINGLE}},
            clear=True,
        ):
            assert reader._get_comet_address("testchain", market_id="usdc") == _SINGLE

    def test_known_chain_with_unknown_market_id_returns_none(self):
        reader = CompoundV3BalanceReader()
        with patch.dict(
            _COMET_ADDRESSES_PATCH_TARGET,
            {"testchain": {"usdc": _SINGLE}},
            clear=True,
        ):
            assert reader._get_comet_address("testchain", market_id="not_a_market") is None


def _make_gateway_with_rpc(
    response_success: bool = True,
    response_result: str | None = '"0x0000000000000000000000000000000000000000000000000000000000000064"',
    timeout: int = 7,
    raise_on_call: bool = False,
):
    """Build a fake gateway_client exposing _rpc_stub with a Call() method."""
    rpc_stub = MagicMock()
    rpc_response = MagicMock()
    rpc_response.success = response_success
    rpc_response.result = response_result
    if raise_on_call:
        rpc_stub.Call.side_effect = RuntimeError("boom")
    else:
        rpc_stub.Call.return_value = rpc_response
    gw = MagicMock()
    gw._rpc_stub = rpc_stub
    gw.config = SimpleNamespace(timeout=timeout)
    return gw, rpc_stub


class TestCompoundV3EthCall:
    """Exercise the _eth_call path and the balance conversion logic."""

    def test_eth_call_returns_hex_on_success(self):
        reader = CompoundV3BalanceReader()
        gw, rpc_stub = _make_gateway_with_rpc()
        out = reader._eth_call(gw, "arbitrum", "0xComet", "0xdeadbeef")
        assert out is not None
        assert out.startswith("0x")
        rpc_stub.Call.assert_called_once()

    def test_eth_call_returns_none_when_no_rpc_stub(self):
        reader = CompoundV3BalanceReader()
        gw = SimpleNamespace()  # no _rpc_stub attribute
        assert reader._eth_call(gw, "arbitrum", "0xComet", "0xabc") is None

    def test_eth_call_returns_none_on_failure_response(self):
        reader = CompoundV3BalanceReader()
        gw, _ = _make_gateway_with_rpc(response_success=False)
        assert reader._eth_call(gw, "arbitrum", "0xComet", "0xabc") is None

    def test_eth_call_returns_none_when_result_empty(self):
        reader = CompoundV3BalanceReader()
        gw, _ = _make_gateway_with_rpc(response_result="")
        assert reader._eth_call(gw, "arbitrum", "0xComet", "0xabc") is None

    def test_eth_call_swallows_exception_and_returns_none(self):
        reader = CompoundV3BalanceReader()
        gw, _ = _make_gateway_with_rpc(raise_on_call=True)
        assert reader._eth_call(gw, "arbitrum", "0xComet", "0xabc") is None

    def test_query_balance_returns_int_on_success(self):
        reader = CompoundV3BalanceReader()
        # hex for 100 = 0x64, padded to 32 bytes
        hex_value = '"' + "0x" + "00" * 31 + "64" + '"'
        gw, _ = _make_gateway_with_rpc(response_result=hex_value)
        value = reader._query_balance(
            reader._BALANCE_OF_SELECTOR, "arbitrum", "0xWallet", "usdc", gw
        )
        assert value == 100

    def test_query_balance_handles_malformed_hex(self):
        reader = CompoundV3BalanceReader()
        gw, _ = _make_gateway_with_rpc(response_result='"not-hex-at-all"')
        value = reader._query_balance(
            reader._BALANCE_OF_SELECTOR, "arbitrum", "0xWallet", "usdc", gw
        )
        assert value is None

    def test_query_balance_returns_none_when_comet_unknown(self):
        reader = CompoundV3BalanceReader()
        gw = MagicMock()
        value = reader._query_balance(
            reader._BALANCE_OF_SELECTOR,
            "arbitrum",
            "0xWallet",
            "not_a_market",
            gw,
        )
        assert value is None

    def test_query_balance_returns_none_when_eth_call_returns_none(self):
        reader = CompoundV3BalanceReader()
        gw, _ = _make_gateway_with_rpc(response_success=False)
        value = reader._query_balance(
            reader._BALANCE_OF_SELECTOR, "arbitrum", "0xWallet", "usdc", gw
        )
        assert value is None

    def test_get_supply_balance_end_to_end(self):
        reader = CompoundV3BalanceReader()
        hex_value = '"' + "0x" + "00" * 31 + "01" + '"'
        gw, _ = _make_gateway_with_rpc(response_result=hex_value)
        value = reader.get_supply_balance(
            "arbitrum", "0xToken", "0xWallet", market_id="usdc", gateway_client=gw
        )
        assert value == 1

    def test_get_debt_balance_end_to_end(self):
        reader = CompoundV3BalanceReader()
        hex_value = '"' + "0x" + "00" * 31 + "ff" + '"'
        gw, _ = _make_gateway_with_rpc(response_result=hex_value)
        value = reader.get_debt_balance(
            "arbitrum", "0xToken", "0xWallet", market_id="usdc", gateway_client=gw
        )
        assert value == 255


# =============================================================================
# resolve_amount_all - NOT_APPLICABLE intent types
# =============================================================================


class TestResolveAmountAllNotApplicable:
    """Intent types outside the resolution table should pass through."""

    def test_unknown_intent_type_passes_through(self):
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = False
        intent.repay_full = False
        intent.intent_type = SimpleNamespace(value="UNKNOWN_TYPE")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_plain_string_intent_type_is_upper_cased(self):
        """intent_type may be a plain string (no .value attribute) and must still
        be looked up via its uppercase form."""
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = False
        intent.repay_full = False
        intent.intent_type = "unknown_type"  # lower-case string
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent


# =============================================================================
# resolve_amount_all - token-address resolution failures
# =============================================================================


class TestResolveAmountAllTokenAddressFailures:
    """Failed token-address resolution must trigger the withdraw_all/repay_full fallback."""

    def _make_withdraw_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def _make_repay_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_withdraw_unresolvable_token_sets_withdraw_all(self, mock_resolve):
        mock_resolve.return_value = None
        intent = self._make_withdraw_intent(protocol="aave_v3", token="WEIRD")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True
        assert result.amount == Decimal("0")

    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_repay_unresolvable_token_sets_repay_full(self, mock_resolve):
        mock_resolve.return_value = None
        intent = self._make_repay_intent(protocol="aave_v3", token="WEIRD")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.repay_full is True
        assert result.amount == Decimal("0")


# =============================================================================
# resolve_amount_all - decimals-lookup failures
# =============================================================================


class TestResolveAmountAllDecimalsFailures:
    """If token-decimals lookup raises, the resolver must fall back safely."""

    def _make_withdraw_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def _make_repay_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    @patch("almanak.framework.intents.amount_resolver._get_token_decimals")
    def test_withdraw_decimals_error_sets_withdraw_all(
        self, mock_decimals, mock_resolve, mock_supply
    ):
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = 5_000_000
        mock_decimals.side_effect = RuntimeError("unknown decimals")

        intent = self._make_withdraw_intent()
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_debt_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    @patch("almanak.framework.intents.amount_resolver._get_token_decimals")
    def test_repay_decimals_error_sets_repay_full(
        self, mock_decimals, mock_resolve, mock_debt
    ):
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_debt.return_value = 5_000_000
        mock_decimals.side_effect = RuntimeError("unknown decimals")

        intent = self._make_repay_intent()
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.repay_full is True


# =============================================================================
# resolve_amount_all - repay happy-path and edge cases
# =============================================================================


class TestResolveAmountAllRepayResolution:
    """Exercise the PROTOCOL_DEBT resolution branch in full."""

    def _make_repay_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_debt_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    @patch("almanak.framework.intents.amount_resolver._get_token_decimals")
    def test_repay_aave_resolves_concrete_amount(
        self, mock_decimals, mock_resolve, mock_debt
    ):
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_debt.return_value = 250_000_000  # 250 USDC with 6 decimals
        mock_decimals.return_value = 6

        intent = self._make_repay_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.amount == Decimal("250")
        assert result.repay_full is False

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_debt_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_repay_none_balance_falls_back_to_repay_full(self, mock_resolve, mock_debt):
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_debt.return_value = None

        intent = self._make_repay_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.repay_full is True

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_debt_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_repay_zero_balance_sets_repay_full(self, mock_resolve, mock_debt):
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_debt.return_value = 0

        intent = self._make_repay_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.repay_full is True


# =============================================================================
# Helper: _resolve_token_address
# =============================================================================


class TestResolveTokenAddress:
    """Test the internal token-address resolver helper."""

    def test_happy_path_returns_resolved_address(self):
        # "USDC" on arbitrum resolves via the real static registry.
        addr = _resolve_token_address("USDC", "arbitrum")
        assert isinstance(addr, str)
        assert addr.lower().startswith("0x")
        assert len(addr) == 42

    def test_unknown_symbol_with_address_like_input_returns_as_is(self):
        """If the symbol can't be resolved but it looks like an address, return it verbatim."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        assert _resolve_token_address(addr, "arbitrum") == addr

    def test_unknown_symbol_non_address_returns_none(self):
        assert _resolve_token_address("DEFINITELY_NOT_A_TOKEN", "arbitrum") is None

    @patch("almanak.framework.intents.amount_resolver.logger")
    def test_unexpected_exception_with_address_fallback(self, _logger):
        """An unexpected exception should still allow an address-like token to pass through."""
        addr = "0x" + "ab" * 20
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=RuntimeError("boom"),
        ):
            assert _resolve_token_address(addr, "arbitrum") == addr

    @patch("almanak.framework.intents.amount_resolver.logger")
    def test_unexpected_exception_non_address_returns_none(self, _logger):
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=RuntimeError("boom"),
        ):
            assert _resolve_token_address("PLAIN_SYMBOL", "arbitrum") is None


# =============================================================================
# Helper: _get_token_decimals
# =============================================================================


class TestGetTokenDecimals:
    """Test the internal decimals helper."""

    def test_returns_decimals_for_known_token(self):
        assert _get_token_decimals("USDC", "arbitrum") == 6

    def test_raises_for_unknown_token(self):
        """Per codebase guideline: never default to 18. Must raise TokenNotFoundError."""
        from almanak.framework.data.tokens.resolver import TokenNotFoundError

        with pytest.raises(TokenNotFoundError):
            _get_token_decimals("DEFINITELY_NOT_A_TOKEN", "arbitrum")


# =============================================================================
# Helpers: _set_resolved_amount, _set_withdraw_all, _set_repay_full
# =============================================================================


class TestSetHelpers:
    """Test the amount-mutating helpers."""

    def _make_withdraw_intent(self):
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount="all",
            chain="arbitrum",
        )

    def _make_repay_intent(self):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount="all",
            chain="arbitrum",
        )

    def test_set_resolved_amount_returns_intent_with_concrete_amount(self):
        intent = self._make_withdraw_intent()
        new = _set_resolved_amount(intent, Decimal("42.5"))
        assert new.amount == Decimal("42.5")

    def test_set_resolved_amount_falls_back_to_model_copy(self):
        """If Intent.set_resolved_amount raises, the helper must fall back to model_copy."""
        intent = self._make_withdraw_intent()
        with patch(
            "almanak.framework.intents.Intent.set_resolved_amount",
            side_effect=TypeError("forced"),
        ):
            new = _set_resolved_amount(intent, Decimal("7"))
        assert new.amount == Decimal("7")

    def test_set_resolved_amount_returns_original_when_no_model_copy(self):
        """If Intent.set_resolved_amount raises AND intent has no model_copy, return original."""

        class Plain:
            amount = "all"

        obj = Plain()
        with patch(
            "almanak.framework.intents.Intent.set_resolved_amount",
            side_effect=TypeError("forced"),
        ):
            result = _set_resolved_amount(obj, Decimal("7"))
        assert result is obj

    def test_set_withdraw_all_sets_flag_and_zero_amount(self):
        intent = self._make_withdraw_intent()
        new = _set_withdraw_all(intent)
        assert new.withdraw_all is True
        assert new.amount == Decimal("0")

    def test_set_withdraw_all_on_object_without_model_copy_returns_original(self):
        class Plain:
            pass

        obj = Plain()
        assert _set_withdraw_all(obj) is obj

    @patch("almanak.framework.intents.amount_resolver.logger")
    def test_set_withdraw_all_swallows_exception(self, mock_logger):
        """If model_copy itself raises, the helper must swallow and return the original."""
        intent = MagicMock()
        intent.model_copy.side_effect = ValueError("forced")
        result = _set_withdraw_all(intent)
        assert result is intent
        assert mock_logger.warning.called

    def test_set_repay_full_sets_flag_and_zero_amount(self):
        intent = self._make_repay_intent()
        new = _set_repay_full(intent)
        assert new.repay_full is True
        assert new.amount == Decimal("0")

    def test_set_repay_full_on_object_without_model_copy_returns_original(self):
        class Plain:
            pass

        obj = Plain()
        assert _set_repay_full(obj) is obj

    @patch("almanak.framework.intents.amount_resolver.logger")
    def test_set_repay_full_swallows_exception(self, mock_logger):
        intent = MagicMock()
        intent.model_copy.side_effect = ValueError("forced")
        result = _set_repay_full(intent)
        assert result is intent
        assert mock_logger.warning.called


# =============================================================================
# Reader registry - additional protocol coverage
# =============================================================================


class TestReaderRegistryAdditional:
    """Cover the remaining supported-protocol aliases."""

    def test_morpho_short_alias(self):
        assert isinstance(get_reader_for_protocol("morpho"), MorphoBlueBalanceReader)

    def test_case_insensitive_lookup(self):
        """Protocol lookup must lower-case the input."""
        assert isinstance(get_reader_for_protocol("AAVE_V3"), AaveV3BalanceReader)


# =============================================================================
# resolve_amount_all - additional withdraw edge cases
# =============================================================================


class TestResolveAmountAllWithdrawAdditional:
    """Cover the branches in the PROTOCOL_SUPPLY resolution path not hit elsewhere."""

    def _make_withdraw_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_withdraw_reader_returns_none_uses_withdraw_all(self, mock_resolve, mock_supply):
        """When reader returns None (couldn't query), fall back to withdraw_all."""
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = None

        intent = self._make_withdraw_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True

    def test_withdraw_morpho_delegates_to_adapter(self):
        """Morpho Blue reader always returns None -> resolver falls back to withdraw_all=True."""
        from almanak.framework.intents.lending_intents import WithdrawIntent

        intent = WithdrawIntent(
            protocol="morpho_blue",
            token="USDC",
            amount="all",
            chain="ethereum",
            market_id="0x" + "a" * 64,  # Morpho requires market_id for isolated markets
        )
        result = resolve_amount_all(
            intent, chain="ethereum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True


# =============================================================================
# resolve_amount_all - Aave-fork protocol routing (Spark data providers)
# =============================================================================

# Ethereum single-reserve data providers, sourced from each connector's
# addresses.py. They are DISTINCT per protocol — that distinction is the whole
# point of the regression below: a Spark amount='all' must NOT query
# Aave V3's contract.
_ETH_SPARK_DATA_PROVIDER = "0xFc21d6d146E6086B8359705C8b28512a983db0cb"
_ETH_AAVE_DATA_PROVIDER = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"


def _gateway_capturing_eth_call_target(
    captured: list[str], *, supply_wei: int = 0, debt_wei: int = 0
):
    """Build a fake gateway whose _rpc_stub.Call records each eth_call target.

    Records ``params[0]["to"]`` (the contract the reader queries) into
    ``captured`` and returns a valid 9-word ``getUserReserveData`` response so
    the full amount='all' resolution path runs end-to-end. ``supply_wei`` lands
    in word 0 (``currentATokenBalance``, drives WITHDRAW/supply resolution) and
    ``debt_wei`` in word 2 (``currentVariableDebt``, which feeds REPAY's
    ``total_debt = stable + variable``).
    """

    def _call(request, timeout=None):
        params = json.loads(request.params)
        captured.append(params[0]["to"])
        words = [0] * 9
        words[0] = supply_wei  # currentATokenBalance
        words[2] = debt_wei  # currentVariableDebt
        hex_payload = "0x" + "".join(f"{w:064x}" for w in words)
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps(hex_payload)
        return resp

    stub = MagicMock()
    stub.Call.side_effect = _call
    gw = MagicMock()
    gw._rpc_stub = stub
    gw.config = SimpleNamespace(timeout=7)
    return gw


# (protocol, its own ethereum data provider, the OTHER fork's provider). The
# third element is what a routing regression would wrongly hit — every case
# asserts both the positive target and that the other fork's address was avoided.
_FORK_ROUTING_CASES = [
    ("spark", _ETH_SPARK_DATA_PROVIDER, _ETH_AAVE_DATA_PROVIDER),
    ("aave_v3", _ETH_AAVE_DATA_PROVIDER, _ETH_SPARK_DATA_PROVIDER),
]


class TestAaveForkProtocolRouting:
    """Regression (follow-up to PR #2533): the protocol the caller resolved must
    be threaded all the way to the on-chain read target.

    ``AaveV3BalanceReader`` serves aave_v3 / spark — Aave forks that
    share the ``getUserReserveData`` ABI but live at DIFFERENT
    ``pool_data_provider`` addresses per chain. Before the protocol was threaded
    through ``AaveV3BalanceReader`` -> ``LendingPositionReader``, a Spark
    WITHDRAW/REPAY amount='all' defaulted to the registry's default protocol
    (aave_v3) and silently queried Aave's contract — wrong balance on every chain
    where the addresses differ. Both the WITHDRAW (supply) and REPAY (debt)
    branches thread the protocol, so both are exercised below. These tests drive
    the real ``LendingReadRegistry`` -> ``AddressRegistry`` -> connector address
    tables, so they fail closed if the routing regresses.
    """

    _WALLET = "0x" + "1" * 40

    def _make_withdraw_intent(self, protocol, token="USDC", chain="ethereum"):
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(protocol=protocol, token=token, amount="all", chain=chain)

    def _make_repay_intent(self, protocol, token="USDC", chain="ethereum"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(protocol=protocol, token=token, amount="all", chain=chain)

    @pytest.mark.parametrize("protocol,expected_provider,other_provider", _FORK_ROUTING_CASES)
    def test_withdraw_routes_supply_query_to_protocol_data_provider(
        self, protocol, expected_provider, other_provider
    ):
        """A WITHDRAW amount='all' reads supply from the resolved protocol's own
        pool_data_provider — Spark must not fall back to Aave's, and the
        aave_v3 case proves the routing is protocol-sensitive, not hardcoded."""
        captured: list[str] = []
        gw = _gateway_capturing_eth_call_target(captured, supply_wei=100_000_000)

        intent = self._make_withdraw_intent(protocol=protocol)
        result = resolve_amount_all(
            intent, chain="ethereum", wallet_address=self._WALLET, gateway_client=gw
        )

        assert captured, f"{protocol} withdraw resolution made no eth_call"
        assert captured[0].lower() == expected_provider.lower()
        assert captured[0].lower() != other_provider.lower()
        # End-to-end: 100_000_000 wei / 1e6 (USDC) = 100 USDC supply resolved.
        assert result.amount == Decimal("100")
        assert result.withdraw_all is False

    @pytest.mark.parametrize("protocol,expected_provider,other_provider", _FORK_ROUTING_CASES)
    def test_repay_routes_debt_query_to_protocol_data_provider(
        self, protocol, expected_provider, other_provider
    ):
        """A REPAY amount='all' reads debt from the resolved protocol's own
        pool_data_provider — the debt branch threads protocol exactly like the
        supply branch, so a Spark repay must not query Aave's contract."""
        captured: list[str] = []
        gw = _gateway_capturing_eth_call_target(captured, debt_wei=250_000_000)

        intent = self._make_repay_intent(protocol=protocol)
        result = resolve_amount_all(
            intent, chain="ethereum", wallet_address=self._WALLET, gateway_client=gw
        )

        assert captured, f"{protocol} repay resolution made no eth_call"
        assert captured[0].lower() == expected_provider.lower()
        assert captured[0].lower() != other_provider.lower()
        # End-to-end: 250_000_000 wei / 1e6 (USDC) = 250 USDC debt resolved.
        assert result.amount == Decimal("250")
        assert result.repay_full is False
