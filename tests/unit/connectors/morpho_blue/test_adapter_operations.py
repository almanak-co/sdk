"""Unit tests for MorphoBlueAdapter operations (supply/withdraw/borrow/repay/etc.).

Covers the success and error paths for all on-chain operations that build
calldata, including health-factor calculations and approval transactions.
All RPC, SDK, and TokenResolver interactions are mocked.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_blue.adapter import (
    MORPHO_BORROW_SELECTOR,
    MORPHO_FLASH_LOAN_SELECTOR,
    MORPHO_LIQUIDATE_SELECTOR,
    MORPHO_SET_AUTHORIZATION_SELECTOR,
    MORPHO_SUPPLY_COLLATERAL_SELECTOR,
    MORPHO_SUPPLY_SELECTOR,
    MORPHO_WITHDRAW_COLLATERAL_SELECTOR,
    MORPHO_WITHDRAW_SELECTOR,
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBlueHealthFactor,
    MorphoBlueMarketParams,
    MorphoBluePosition,
    create_adapter_with_prices,
    create_test_adapter,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

WSTETH_USDC_MARKET = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
TEST_WALLET = "0x1234567890123456789012345678901234567890"
OTHER_WALLET = "0x9876543210987654321098765432109876543210"


def _make_resolver(symbol: str = "USDC", address: str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", decimals: int = 6) -> MagicMock:
    """Build a mock TokenResolver that returns a fixed ResolvedToken regardless of symbol."""
    resolver = MagicMock()

    def _resolve(token: str, chain: str) -> ResolvedToken:
        return ResolvedToken(
            symbol=symbol,
            address=address,
            decimals=decimals,
            chain=chain,
            chain_id=1,
        )

    resolver.resolve.side_effect = _resolve
    return resolver


@pytest.fixture
def usdc_resolver() -> MagicMock:
    return _make_resolver("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6)


@pytest.fixture
def wsteth_resolver() -> MagicMock:
    return _make_resolver("wstETH", "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 18)


@pytest.fixture
def adapter_no_sdk(usdc_resolver: MagicMock) -> MorphoBlueAdapter:
    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
        enable_sdk=False,
    )
    return MorphoBlueAdapter(config, token_resolver=usdc_resolver)


class TestSupplyOperation:
    """Cover supply() success / shares mode / unknown market / exception paths."""

    def test_supply_assets_mode(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.supply(WSTETH_USDC_MARKET, Decimal("100"))
        assert result.success
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(MORPHO_SUPPLY_SELECTOR)
        assert "USDC" in result.description or "100" in result.description

    def test_supply_shares_mode(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.supply(WSTETH_USDC_MARKET, Decimal("1"), shares_mode=True)
        assert result.success
        assert "shares" in result.description

    def test_supply_on_behalf_of(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.supply(WSTETH_USDC_MARKET, Decimal("10"), on_behalf_of=OTHER_WALLET)
        assert result.success

    def test_supply_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.supply("0x" + "ee" * 32, Decimal("10"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_supply_exception_propagates_as_failure(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.supply(WSTETH_USDC_MARKET, Decimal("10"))
        assert not result.success
        assert result.error is not None


class TestWithdrawOperation:
    """Cover withdraw() success / shares / withdraw_all / errors."""

    def test_withdraw_assets_mode(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.withdraw(WSTETH_USDC_MARKET, Decimal("50"))
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_SELECTOR)

    def test_withdraw_shares_mode(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.withdraw(WSTETH_USDC_MARKET, Decimal("5"), shares_mode=True)
        assert result.success
        assert "shares" in result.description

    def test_withdraw_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.withdraw("0x" + "ee" * 32, Decimal("1"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_withdraw_with_receiver_and_owner(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.withdraw(
            WSTETH_USDC_MARKET, Decimal("5"), receiver=OTHER_WALLET, on_behalf_of=OTHER_WALLET
        )
        assert result.success

    def test_withdraw_exception_propagates_as_failure(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.withdraw(WSTETH_USDC_MARKET, Decimal("1"))
        assert not result.success


class TestSupplyCollateral:
    def test_success(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.supply_collateral(WSTETH_USDC_MARKET, Decimal("1.5"))
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_SUPPLY_COLLATERAL_SELECTOR)
        assert "wstETH" in result.description

    def test_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.supply_collateral("0x" + "ee" * 32, Decimal("1"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_on_behalf_of(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.supply_collateral(
            WSTETH_USDC_MARKET, Decimal("1"), on_behalf_of=OTHER_WALLET
        )
        assert result.success

    def test_exception(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        wsteth_resolver.resolve.side_effect = TokenResolutionError("wstETH", "ethereum", "boom")
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.supply_collateral(WSTETH_USDC_MARKET, Decimal("1"))
        assert not result.success


class TestWithdrawCollateral:
    def test_success(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.withdraw_collateral(WSTETH_USDC_MARKET, Decimal("1"))
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_COLLATERAL_SELECTOR)

    def test_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.withdraw_collateral("0x" + "ee" * 32, Decimal("1"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_with_receiver_and_owner(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.withdraw_collateral(
            WSTETH_USDC_MARKET, Decimal("1"), receiver=OTHER_WALLET, on_behalf_of=OTHER_WALLET
        )
        assert result.success

    def test_exception(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        wsteth_resolver.resolve.side_effect = TokenResolutionError("wstETH", "ethereum", "boom")
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.withdraw_collateral(WSTETH_USDC_MARKET, Decimal("1"))
        assert not result.success


class TestBorrow:
    def test_success(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.borrow(WSTETH_USDC_MARKET, Decimal("100"))
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_BORROW_SELECTOR)
        assert "USDC" in result.description

    def test_shares_mode(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.borrow(WSTETH_USDC_MARKET, Decimal("1"), shares_mode=True)
        assert result.success
        assert "shares" in result.description

    def test_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.borrow("0x" + "ee" * 32, Decimal("100"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_with_receiver_and_owner(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.borrow(
            WSTETH_USDC_MARKET, Decimal("100"), receiver=OTHER_WALLET, on_behalf_of=OTHER_WALLET
        )
        assert result.success

    def test_exception(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.borrow(WSTETH_USDC_MARKET, Decimal("100"))
        assert not result.success


class TestRepay:
    """Beyond the existing repay-guard tests, exercise the unknown-market /
    repay_all-no-position / repay_all success paths."""

    def test_repay_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.repay("0x" + "ee" * 32, Decimal("100"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_repay_all_no_position(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        with patch.object(
            adapter_no_sdk,
            "get_position_on_chain",
            return_value=MorphoBluePosition(market_id=WSTETH_USDC_MARKET),
        ):
            result = adapter_no_sdk.repay(WSTETH_USDC_MARKET, Decimal("0"), repay_all=True)
        assert not result.success
        assert "No borrow position" in result.error

    def test_repay_all_uses_actual_borrow_shares(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET,
            borrow_shares=Decimal("123456789"),
        )
        with patch.object(adapter_no_sdk, "get_position_on_chain", return_value=position):
            result = adapter_no_sdk.repay(WSTETH_USDC_MARKET, Decimal("0"), repay_all=True)
        assert result.success
        assert "full debt" in result.description

    def test_repay_exception(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.repay(WSTETH_USDC_MARKET, Decimal("100"))
        assert not result.success


class TestFlashLoan:
    def test_success(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.flash_loan("USDC", Decimal("1000"))
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_FLASH_LOAN_SELECTOR)

    def test_with_callback_data(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.flash_loan("USDC", Decimal("1000"), callback_data=b"\x12\x34")
        assert result.success

    def test_address_passthrough(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = adapter_no_sdk.flash_loan(addr, Decimal("1000"))
        assert result.success

    def test_exception_path(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        # Cause _resolve_token to fail (TokenResolutionError) for a symbol input.
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.flash_loan("USDC", Decimal("1000"))
        assert not result.success


class TestLiquidate:
    def test_success(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.liquidate(
            WSTETH_USDC_MARKET, OTHER_WALLET, Decimal("0.5")
        )
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_LIQUIDATE_SELECTOR)
        assert "Liquidate" in result.description

    def test_with_repaid_shares(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.liquidate(
            WSTETH_USDC_MARKET, OTHER_WALLET, Decimal("0"), repaid_shares=Decimal("1000")
        )
        assert result.success

    def test_with_callback_data(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.liquidate(
            WSTETH_USDC_MARKET, OTHER_WALLET, Decimal("0.5"), callback_data=b"\xde\xad\xbe\xef"
        )
        assert result.success

    def test_unknown_market(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.liquidate("0x" + "ee" * 32, OTHER_WALLET, Decimal("0.5"))
        assert not result.success
        assert "Unknown market" in result.error

    def test_exception(self, adapter_no_sdk: MorphoBlueAdapter, wsteth_resolver: MagicMock) -> None:
        wsteth_resolver.resolve.side_effect = TokenResolutionError("wstETH", "ethereum", "boom")
        adapter_no_sdk._token_resolver = wsteth_resolver
        result = adapter_no_sdk.liquidate(WSTETH_USDC_MARKET, OTHER_WALLET, Decimal("0.5"))
        assert not result.success


class TestSetAuthorization:
    def test_authorize(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.set_authorization(OTHER_WALLET, True)
        assert result.success
        assert result.tx_data["data"].startswith(MORPHO_SET_AUTHORIZATION_SELECTOR)
        assert "Authorize" in result.description

    def test_deauthorize(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.set_authorization(OTHER_WALLET, False)
        assert result.success
        assert "Deauthorize" in result.description


class TestApproveTransaction:
    def test_default_max_amount(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.build_approve_transaction("USDC")
        assert result.success
        assert result.tx_data["data"].startswith("0x095ea7b3")
        assert "unlimited" in result.description

    def test_specific_amount(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.build_approve_transaction("USDC", amount=Decimal("100"))
        assert result.success
        assert "100" in result.description

    def test_custom_spender(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk.build_approve_transaction(
            "USDC", amount=Decimal("100"), spender=OTHER_WALLET
        )
        assert result.success

    def test_exception(self, adapter_no_sdk: MorphoBlueAdapter, usdc_resolver: MagicMock) -> None:
        usdc_resolver.resolve.side_effect = TokenResolutionError("USDC", "ethereum", "boom")
        result = adapter_no_sdk.build_approve_transaction("USDC")
        assert not result.success


class TestMarketInformation:
    def test_get_market_info_known(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        info = adapter_no_sdk.get_market_info(WSTETH_USDC_MARKET)
        assert info is not None
        assert info["loan_token"] == "USDC"

    def test_get_market_info_unknown_no_sdk(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        info = adapter_no_sdk.get_market_info("0x" + "ee" * 32)
        assert info is None

    def test_get_markets_returns_copy(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        markets = adapter_no_sdk.get_markets()
        assert isinstance(markets, dict)
        markets["junk"] = {}
        # Original should not be polluted
        assert "junk" not in adapter_no_sdk.markets

    def test_get_market_params_known(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        params = adapter_no_sdk.get_market_params(WSTETH_USDC_MARKET)
        assert params is not None
        assert isinstance(params, MorphoBlueMarketParams)

    def test_get_market_params_unknown(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        params = adapter_no_sdk.get_market_params("0x" + "ee" * 32)
        assert params is None


class TestHealthFactor:
    def test_healthy(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        hf = adapter_no_sdk.calculate_health_factor(
            collateral_amount=Decimal("10"),
            collateral_price_usd=Decimal("2000"),
            debt_amount=Decimal("10000"),
            debt_price_usd=Decimal("1"),
            lltv=Decimal("0.86"),
        )
        assert hf.is_healthy
        assert hf.health_factor > 1
        assert hf.max_borrow_usd > 0

    def test_unhealthy(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        hf = adapter_no_sdk.calculate_health_factor(
            collateral_amount=Decimal("1"),
            collateral_price_usd=Decimal("2000"),
            debt_amount=Decimal("5000"),
            debt_price_usd=Decimal("1"),
            lltv=Decimal("0.86"),
        )
        assert not hf.is_healthy
        # max_borrow_usd is clamped to 0 when negative
        assert hf.max_borrow_usd == Decimal("0")

    def test_zero_debt_returns_infinite_hf(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        hf = adapter_no_sdk.calculate_health_factor(
            collateral_amount=Decimal("1"),
            collateral_price_usd=Decimal("2000"),
            debt_amount=Decimal("0"),
            debt_price_usd=Decimal("1"),
            lltv=Decimal("0.86"),
        )
        assert hf.health_factor == Decimal("999999")

    def test_to_dict_and_props(self) -> None:
        hf = MorphoBlueHealthFactor(
            collateral_value_usd=Decimal("1000"),
            debt_value_usd=Decimal("100"),
            lltv=Decimal("0.86"),
            health_factor=Decimal("8.6"),
        )
        as_dict = hf.to_dict()
        assert "is_healthy" in as_dict
        assert "liquidation_threshold_usd" in as_dict
        assert hf.is_healthy


class TestPriceOracleFromDict:
    def test_exact_match(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            price_provider={"USDC": Decimal("1.0"), "wstETH": Decimal("3500")},
            enable_sdk=False,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=_make_resolver())
        assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_uppercase_match(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            price_provider={"USDC": Decimal("1.0")},
            enable_sdk=False,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=_make_resolver())
        assert adapter._price_oracle("usdc") == Decimal("1.0")

    def test_lowercase_match(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            price_provider={"usdc": Decimal("1.0")},
            enable_sdk=False,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=_make_resolver())
        assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_missing_returns_zero(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            price_provider={"USDC": Decimal("1.0")},
            enable_sdk=False,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=_make_resolver())
        assert adapter._price_oracle("UNKNOWN") == Decimal("0")

    def test_default_oracle_returns_one(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        # adapter_no_sdk uses default placeholder oracle
        assert adapter_no_sdk._default_price_oracle("FOO") == Decimal("1.0")


class TestFactoryFunctions:
    def test_create_test_adapter(self) -> None:
        adapter = create_test_adapter()
        assert adapter.chain == "ethereum"
        assert adapter._using_placeholder_prices

    def test_create_adapter_with_prices_lookup_paths(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            enable_sdk=False,
        )
        prices = {"USDC": Decimal("1"), "wsteth": Decimal("3500")}
        adapter = create_adapter_with_prices(config, prices)
        assert adapter._price_oracle("USDC") == Decimal("1")
        assert adapter._price_oracle("usdc") == Decimal("1")  # case-insensitive
        assert adapter._price_oracle("WSTETH") == Decimal("3500")  # uppercase fallback

    def test_create_adapter_with_prices_missing_raises(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            enable_sdk=False,
        )
        adapter = create_adapter_with_prices(config, {"USDC": Decimal("1")})
        with pytest.raises(KeyError):
            adapter._price_oracle("UNKNOWN")


class TestConfigValidation:
    def test_invalid_chain_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid chain"):
            MorphoBlueConfig(chain="bogus_chain", wallet_address=TEST_WALLET)

    def test_invalid_wallet_address_no_prefix(self) -> None:
        with pytest.raises(ValueError, match="Invalid wallet address"):
            MorphoBlueConfig(chain="ethereum", wallet_address="1234")

    def test_invalid_wallet_address_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="Invalid wallet address"):
            MorphoBlueConfig(chain="ethereum", wallet_address="0x12")

    def test_invalid_slippage_negative(self) -> None:
        with pytest.raises(ValueError, match="Invalid slippage"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address=TEST_WALLET,
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_too_large(self) -> None:
        with pytest.raises(ValueError, match="Invalid slippage"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address=TEST_WALLET,
                default_slippage_bps=100000,
            )


class TestSDKLazyInitGuard:
    def test_sdk_disabled_raises(self) -> None:
        adapter = create_test_adapter()
        with pytest.raises(RuntimeError, match="SDK is disabled"):
            _ = adapter.sdk

    def test_default_oracle_warning_path(self) -> None:
        """Backwards-compat: no oracle, no provider, no allow_placeholder_prices
        still constructs but emits a warning and uses placeholder oracle."""
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            enable_sdk=False,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=_make_resolver())
        assert adapter._using_placeholder_prices


class TestOnChainReadHelpers:
    """Adapter facades that delegate to SDK."""

    def test_get_position_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        sdk_position = MagicMock()
        sdk_position.market_id = WSTETH_USDC_MARKET
        sdk_position.supply_shares = 1000
        sdk_position.borrow_shares = 500
        sdk_position.collateral = 1
        adapter_no_sdk._sdk.get_position.return_value = sdk_position

        position = adapter_no_sdk.get_position_on_chain(WSTETH_USDC_MARKET)
        assert position.supply_shares == Decimal("1000")
        assert position.borrow_shares == Decimal("500")

    def test_get_market_state_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        sdk_state = MagicMock()
        sdk_state.market_id = WSTETH_USDC_MARKET
        sdk_state.total_supply_assets = 1000
        sdk_state.total_supply_shares = 1000
        sdk_state.total_borrow_assets = 500
        sdk_state.total_borrow_shares = 500
        sdk_state.last_update = 1234
        sdk_state.fee = 0
        adapter_no_sdk._sdk.get_market_state.return_value = sdk_state

        state = adapter_no_sdk.get_market_state_on_chain(WSTETH_USDC_MARKET)
        assert state.last_update == 1234

    def test_get_market_params_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        sdk_params = MagicMock()
        sdk_params.loan_token = "0x1"
        sdk_params.collateral_token = "0x2"
        sdk_params.oracle = "0x3"
        sdk_params.irm = "0x4"
        sdk_params.lltv = 1
        adapter_no_sdk._sdk.get_market_params.return_value = sdk_params

        p = adapter_no_sdk.get_market_params_on_chain(WSTETH_USDC_MARKET)
        assert p.loan_token == "0x1"

    def test_discover_markets_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        adapter_no_sdk._sdk.discover_markets.return_value = ["0xabc"]
        assert adapter_no_sdk.discover_markets_on_chain() == ["0xabc"]

    def test_get_supply_assets_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        adapter_no_sdk._sdk.get_supply_assets.return_value = 999
        result = adapter_no_sdk.get_supply_assets_on_chain(WSTETH_USDC_MARKET)
        assert result == Decimal("999")

    def test_get_borrow_assets_on_chain(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        adapter_no_sdk._sdk.get_borrow_assets.return_value = 555
        result = adapter_no_sdk.get_borrow_assets_on_chain(WSTETH_USDC_MARKET)
        assert result == Decimal("555")


class TestGetMarketInfoOnChainFallback:
    """Cover the SDK fallback in _get_market_info."""

    def test_unknown_market_resolves_via_sdk(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        sdk_params = MagicMock()
        sdk_params.loan_token = "0xa1"
        sdk_params.collateral_token = "0xa2"
        sdk_params.oracle = "0xa3"
        sdk_params.irm = "0xa4"
        sdk_params.lltv = 860000000000000000
        sdk_params.lltv_percent = Decimal("86.0")
        adapter_no_sdk._sdk.get_market_params.return_value = sdk_params

        unknown_id = "0x" + "ee" * 32
        info = adapter_no_sdk._get_market_info(unknown_id)
        assert info is not None
        assert info["loan_token"] == "0xa1"
        assert info["name"].startswith("on-chain:")

    def test_sdk_lookup_failure_returns_none(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        adapter_no_sdk._sdk_enabled = True
        adapter_no_sdk._sdk = MagicMock()
        adapter_no_sdk._sdk.get_market_params.side_effect = Exception("RPC down")

        unknown_id = "0x" + "ee" * 32
        info = adapter_no_sdk._get_market_info(unknown_id)
        assert info is None

    def test_market_id_normalization(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        # No 0x prefix + uppercase
        no_prefix = WSTETH_USDC_MARKET[2:].upper()
        info = adapter_no_sdk._get_market_info(no_prefix)
        assert info is not None
        assert info["loan_token"] == "USDC"


class TestEncodeBytesNonEmpty:
    """Cover the non-empty branch of _encode_bytes."""

    def test_non_empty_bytes(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        offset, tail = adapter_no_sdk._encode_bytes(b"\x01\x02\x03", static_slots=8)
        # Offset = 8 * 32 = 256, hex-padded
        assert int(offset, 16) == 256
        # Length = 3 in first 32 bytes, then padded data
        assert int(tail[:64], 16) == 3
        assert tail[64:70] == "010203"

    def test_empty_bytes(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        offset, tail = adapter_no_sdk._encode_bytes(b"", static_slots=8)
        assert int(tail, 16) == 0

    def test_pad_uint16(self, adapter_no_sdk: MorphoBlueAdapter) -> None:
        result = adapter_no_sdk._pad_uint16(42)
        assert int(result, 16) == 42
        assert len(result) == 64
