"""Tests for AaveV3Adapter intent-builder methods.

Covers transaction-builder branches for supply / withdraw / borrow / repay /
set_user_use_reserve_as_collateral / set_user_emode / flash_loan / flash_loan_simple /
liquidation_call / build_approve_tx, including:

- happy paths (calldata layout, gas estimate, description)
- on_behalf_of override vs default-to-wallet
- withdraw_all / repay_all -> MAX_UINT256
- unknown asset -> TransactionResult(success=False)
- exception path -> TransactionResult(success=False, error=str(e))
- flash loan length-mismatch + per-leg unknown asset
- E-Mode known + unknown category descriptions

Pattern mirrors existing test_aave_v3_adapter_resolver.py — config + mock
TokenResolver, no real network.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aave_v3.adapter import (
    AAVE_BORROW_SELECTOR,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_LIQUIDATION_CALL_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_USER_EMODE_SELECTOR,
    AAVE_SET_USER_USE_RESERVE_AS_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_V3_POOL_ADDRESSES,
    AAVE_WITHDRAW_SELECTOR,
    DEFAULT_GAS_ESTIMATES,
    ERC20_APPROVE_SELECTOR,
    MAX_UINT256,
    AaveV3Adapter,
    AaveV3Config,
    AaveV3InterestRateMode,
    AaveV3ReserveData,
)
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"
RECIPIENT = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"


@pytest.fixture
def config() -> AaveV3Config:
    return AaveV3Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


def _resolver_for(symbol_to_token: dict[str, ResolvedToken]) -> MagicMock:
    """Build a MagicMock TokenResolver that maps symbol -> ResolvedToken."""
    resolver = MagicMock()

    def _resolve(asset: str, _chain: str) -> ResolvedToken:
        if asset in symbol_to_token:
            return symbol_to_token[asset]
        raise KeyError(asset)

    resolver.resolve.side_effect = _resolve
    return resolver


@pytest.fixture
def usdc_resolver() -> MagicMock:
    return _resolver_for(
        {
            "USDC": ResolvedToken(
                symbol="USDC",
                address=USDC_ADDRESS,
                decimals=6,
                chain="arbitrum",
                chain_id=42161,
            ),
            "WETH": ResolvedToken(
                symbol="WETH",
                address=WETH_ADDRESS,
                decimals=18,
                chain="arbitrum",
                chain_id=42161,
            ),
        }
    )


@pytest.fixture
def adapter(config: AaveV3Config, usdc_resolver: MagicMock) -> AaveV3Adapter:
    return AaveV3Adapter(config, token_resolver=usdc_resolver)


@pytest.fixture
def boom_adapter(config: AaveV3Config) -> AaveV3Adapter:
    """Adapter whose decimals lookup blows up — exercises the except-path."""
    resolver = MagicMock()
    resolver.resolve.side_effect = RuntimeError("boom")
    return AaveV3Adapter(config, token_resolver=resolver)


def _pool() -> str:
    return AAVE_V3_POOL_ADDRESSES["arbitrum"]


# =============================================================================
# Supply
# =============================================================================


class TestSupply:
    def test_supply_happy_path_defaults_to_wallet(self, adapter: AaveV3Adapter) -> None:
        result = adapter.supply("USDC", Decimal("100"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == _pool()
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(AAVE_SUPPLY_SELECTOR)
        # 100 USDC at 6 decimals = 100_000_000 → embedded as last 64 hex chars of amount field
        assert hex(100 * 10**6)[2:] in result.tx_data["data"]
        # Default on_behalf_of must equal wallet (lower-cased and zero-padded)
        assert TEST_WALLET[2:].lower() in result.tx_data["data"]
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["supply"]
        assert "Supply 100 USDC" in result.description

    def test_supply_on_behalf_of_override(self, adapter: AaveV3Adapter) -> None:
        result = adapter.supply("USDC", Decimal("1"), on_behalf_of=RECIPIENT)
        assert result.success is True
        assert RECIPIENT[2:].lower() in result.tx_data["data"]

    def test_supply_address_passthrough(self, config: AaveV3Config) -> None:
        # 0x-prefixed 42-char address must skip the resolver in _resolve_asset
        # (the address-passthrough branch at adapter.py:1570). Decimals lookup
        # still goes through the resolver, so we register the address there.
        usdc_token = ResolvedToken(
            symbol="USDC",
            address=USDC_ADDRESS,
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        resolver = _resolver_for({USDC_ADDRESS: usdc_token})
        adapter = AaveV3Adapter(config, token_resolver=resolver)

        result = adapter.supply(USDC_ADDRESS, Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        # Calldata embeds USDC's address (lowercased, zero-padded)
        assert USDC_ADDRESS[2:].lower() in result.tx_data["data"]
        # 1 USDC at 6 decimals = 1_000_000
        assert hex(1 * 10**6)[2:] in result.tx_data["data"]
        assert result.tx_data["data"].startswith(AAVE_SUPPLY_SELECTOR)
        # _resolve_asset must NOT have been called with the address (passthrough branch).
        # _get_decimals DOES call resolver.resolve once with the address.
        resolve_calls = [c.args for c in resolver.resolve.call_args_list]
        assert resolve_calls == [(USDC_ADDRESS, "arbitrum")], (
            f"Expected exactly one resolve call from _get_decimals; got {resolve_calls}"
        )

    def test_supply_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.supply("USDC", Decimal("1"))
        assert result.success is False
        assert result.error is not None
        assert result.tx_data is None


# =============================================================================
# Withdraw
# =============================================================================


class TestWithdraw:
    def test_withdraw_specific_amount(self, adapter: AaveV3Adapter) -> None:
        result = adapter.withdraw("USDC", Decimal("50"))
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_WITHDRAW_SELECTOR)
        assert hex(50 * 10**6)[2:] in result.tx_data["data"]
        assert "Withdraw 50 USDC" in result.description

    def test_withdraw_all_uses_max_uint256(self, adapter: AaveV3Adapter) -> None:
        result = adapter.withdraw("USDC", Decimal("0"), withdraw_all=True)
        assert result.success is True
        # MAX_UINT256 = 0xff..ff (64 hex chars)
        assert "f" * 64 in result.tx_data["data"]
        assert "Withdraw all USDC" in result.description

    def test_withdraw_to_recipient(self, adapter: AaveV3Adapter) -> None:
        result = adapter.withdraw("USDC", Decimal("1"), to=RECIPIENT)
        assert result.success is True
        assert RECIPIENT[2:].lower() in result.tx_data["data"]

    def test_withdraw_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.withdraw("USDC", Decimal("1"))
        assert result.success is False
        assert result.tx_data is None


# =============================================================================
# Borrow
# =============================================================================


class TestBorrow:
    def test_borrow_variable_rate_default(self, adapter: AaveV3Adapter) -> None:
        result = adapter.borrow("USDC", Decimal("100"))
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_BORROW_SELECTOR)
        # mode = 2 (VARIABLE)
        assert "0" * 63 + "2" in result.tx_data["data"]
        assert "variable rate" in result.description

    def test_borrow_stable_rate(self, adapter: AaveV3Adapter) -> None:
        result = adapter.borrow(
            "USDC", Decimal("1"), interest_rate_mode=AaveV3InterestRateMode.STABLE
        )
        assert result.success is True
        assert "stable rate" in result.description

    def test_borrow_on_behalf_of(self, adapter: AaveV3Adapter) -> None:
        result = adapter.borrow("USDC", Decimal("1"), on_behalf_of=RECIPIENT)
        assert result.success is True
        assert RECIPIENT[2:].lower() in result.tx_data["data"]

    def test_borrow_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.borrow("USDC", Decimal("1"))
        assert result.success is False


# =============================================================================
# Repay
# =============================================================================


class TestRepay:
    def test_repay_specific_amount(self, adapter: AaveV3Adapter) -> None:
        result = adapter.repay("USDC", Decimal("10"))
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_REPAY_SELECTOR)
        assert "Repay 10 USDC" in result.description

    def test_repay_all_uses_max_uint256(self, adapter: AaveV3Adapter) -> None:
        result = adapter.repay("USDC", Decimal("0"), repay_all=True)
        assert result.success is True
        assert "f" * 64 in result.tx_data["data"]
        assert "Repay full debt" in result.description

    def test_repay_on_behalf_of(self, adapter: AaveV3Adapter) -> None:
        result = adapter.repay("USDC", Decimal("1"), on_behalf_of=RECIPIENT)
        assert result.success is True
        assert RECIPIENT[2:].lower() in result.tx_data["data"]

    def test_repay_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.repay("USDC", Decimal("1"))
        assert result.success is False


# =============================================================================
# setUserUseReserveAsCollateral
# =============================================================================


class TestSetCollateral:
    def test_enable_collateral(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_use_reserve_as_collateral("USDC", True)
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_SET_USER_USE_RESERVE_AS_COLLATERAL_SELECTOR)
        # use_as_collateral=True is encoded as 1
        assert "0" * 63 + "1" in result.tx_data["data"]
        assert "Enable USDC as collateral" in result.description

    def test_disable_collateral(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_use_reserve_as_collateral("USDC", False)
        assert result.success is True
        # use_as_collateral=False is encoded as 0 (last 64 chars all-zero)
        assert result.tx_data["data"].endswith("0" * 64)
        assert "Disable USDC as collateral" in result.description

    def test_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.set_user_use_reserve_as_collateral("USDC", True)
        assert result.success is False


# =============================================================================
# setUserEMode + get_emode_category_data
# =============================================================================


class TestEMode:
    def test_set_emode_none(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_emode(0)
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_SET_USER_EMODE_SELECTOR)
        assert "Set E-Mode to None" in result.description

    def test_set_emode_eth_correlated(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_emode(1)
        assert result.success is True
        assert "ETH Correlated" in result.description

    def test_set_emode_stablecoins(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_emode(2)
        assert result.success is True
        assert "Stablecoins" in result.description

    def test_set_emode_unknown_category_id(self, adapter: AaveV3Adapter) -> None:
        result = adapter.set_user_emode(99)
        assert result.success is True
        assert "Category 99" in result.description

    def test_get_emode_category_data_none(self, adapter: AaveV3Adapter) -> None:
        d = adapter.get_emode_category_data(0)
        assert d["id"] == 0 and d["label"] == "None"

    def test_get_emode_category_data_eth(self, adapter: AaveV3Adapter) -> None:
        d = adapter.get_emode_category_data(1)
        assert d["liquidation_threshold"] == 9500

    def test_get_emode_category_data_stables(self, adapter: AaveV3Adapter) -> None:
        d = adapter.get_emode_category_data(2)
        assert d["liquidation_threshold"] == 9750

    def test_get_emode_category_data_unknown_falls_back_to_none(
        self, adapter: AaveV3Adapter
    ) -> None:
        d = adapter.get_emode_category_data(99)
        assert d["id"] == 0


# =============================================================================
# Flash loan (multi-asset) — covers the largest CC method in adapter
# =============================================================================


class TestFlashLoan:
    def test_flash_loan_single_asset(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC"],
            amounts=[Decimal("1000")],
            modes=[0],
        )
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_FLASH_LOAN_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["flash_loan"]
        assert "1000 USDC" in result.description

    def test_flash_loan_multi_asset(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC", "WETH"],
            amounts=[Decimal("100"), Decimal("0.5")],
            modes=[0, 0],
        )
        assert result.success is True
        # Both asset addresses must appear in calldata (lower-cased, no 0x)
        assert USDC_ADDRESS[2:].lower() in result.tx_data["data"]
        assert WETH_ADDRESS[2:].lower() in result.tx_data["data"]

    def test_flash_loan_with_params_payload(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC"],
            amounts=[Decimal("1")],
            modes=[2],
            params=b"\xde\xad\xbe\xef",
        )
        assert result.success is True
        # 4-byte params -> hex "deadbeef" appears in calldata, padded
        assert "deadbeef" in result.tx_data["data"]

    def test_flash_loan_on_behalf_of_override(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC"],
            amounts=[Decimal("1")],
            modes=[1],
            on_behalf_of=RECIPIENT,
        )
        assert result.success is True
        assert RECIPIENT[2:].lower() in result.tx_data["data"]

    def test_flash_loan_length_mismatch_returns_error(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC", "WETH"],
            amounts=[Decimal("1")],  # mismatch
            modes=[0, 0],
        )
        assert result.success is False
        assert "same length" in result.error

    def test_flash_loan_unknown_asset_in_list_returns_error(
        self, config: AaveV3Config
    ) -> None:
        # Only USDC is registered; WETH lookup will raise
        resolver = _resolver_for(
            {
                "USDC": ResolvedToken(
                    symbol="USDC",
                    address=USDC_ADDRESS,
                    decimals=6,
                    chain="arbitrum",
                    chain_id=42161,
                ),
            }
        )
        adapter = AaveV3Adapter(config, token_resolver=resolver)
        result = adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC", "WETH"],
            amounts=[Decimal("1"), Decimal("1")],
            modes=[0, 0],
        )
        assert result.success is False

    def test_flash_loan_resolver_failure_returns_error(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.flash_loan(
            receiver_address=RECIPIENT,
            assets=["USDC"],
            amounts=[Decimal("1")],
            modes=[0],
        )
        assert result.success is False


class TestFlashLoanSimple:
    def test_flash_loan_simple_no_params(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan_simple(
            receiver_address=RECIPIENT,
            asset="USDC",
            amount=Decimal("100"),
        )
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_FLASH_LOAN_SIMPLE_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["flash_loan_simple"]
        assert "100 USDC" in result.description

    def test_flash_loan_simple_with_params(self, adapter: AaveV3Adapter) -> None:
        result = adapter.flash_loan_simple(
            receiver_address=RECIPIENT,
            asset="USDC",
            amount=Decimal("1"),
            params=b"\xca\xfe",
        )
        assert result.success is True
        assert "cafe" in result.tx_data["data"]

    def test_flash_loan_simple_resolver_failure(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.flash_loan_simple(
            receiver_address=RECIPIENT,
            asset="USDC",
            amount=Decimal("1"),
        )
        assert result.success is False


# =============================================================================
# Liquidation
# =============================================================================


class TestLiquidationCall:
    def test_liquidate_success(self, adapter: AaveV3Adapter) -> None:
        victim = "0x" + "11" * 20
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user=victim,
            debt_to_cover=Decimal("500"),
        )
        assert result.success is True
        assert result.tx_data["data"].startswith(AAVE_LIQUIDATION_CALL_SELECTOR)
        # receive_atoken=False -> last 64 chars all zero
        assert result.tx_data["data"].endswith("0" * 64)
        assert "Liquidate" in result.description

    def test_liquidate_receive_atoken(self, adapter: AaveV3Adapter) -> None:
        victim = "0x" + "11" * 20
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user=victim,
            debt_to_cover=Decimal("1"),
            receive_atoken=True,
        )
        assert result.success is True
        assert "0" * 63 + "1" in result.tx_data["data"]

    def test_liquidate_unknown_collateral(self, config: AaveV3Config) -> None:
        # Resolver only knows USDC -> WETH (collateral) lookup fails
        resolver = _resolver_for(
            {
                "USDC": ResolvedToken(
                    symbol="USDC",
                    address=USDC_ADDRESS,
                    decimals=6,
                    chain="arbitrum",
                    chain_id=42161,
                ),
            }
        )
        adapter = AaveV3Adapter(config, token_resolver=resolver)
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user="0x" + "11" * 20,
            debt_to_cover=Decimal("1"),
        )
        assert result.success is False

    def test_liquidate_unknown_debt(self, config: AaveV3Config) -> None:
        # Resolver only knows WETH -> USDC (debt) lookup fails
        resolver = _resolver_for(
            {
                "WETH": ResolvedToken(
                    symbol="WETH",
                    address=WETH_ADDRESS,
                    decimals=18,
                    chain="arbitrum",
                    chain_id=42161,
                ),
            }
        )
        adapter = AaveV3Adapter(config, token_resolver=resolver)
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user="0x" + "11" * 20,
            debt_to_cover=Decimal("1"),
        )
        assert result.success is False

    def test_liquidate_resolver_failure(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user="0x" + "11" * 20,
            debt_to_cover=Decimal("1"),
        )
        assert result.success is False


# =============================================================================
# build_approve_tx
# =============================================================================


class TestBuildApproveTx:
    def test_approve_specific_amount(self, adapter: AaveV3Adapter) -> None:
        result = adapter.build_approve_tx("USDC", amount=Decimal("100"))
        assert result.success is True
        assert result.tx_data["to"] == USDC_ADDRESS
        assert result.tx_data["data"].startswith(ERC20_APPROVE_SELECTOR)
        # Spender = pool address (must appear in calldata)
        assert _pool()[2:].lower() in result.tx_data["data"]
        assert "100 USDC" in result.description

    def test_approve_unlimited_default(self, adapter: AaveV3Adapter) -> None:
        result = adapter.build_approve_tx("USDC")
        assert result.success is True
        # MAX_UINT256 in hex = ff*64
        assert "f" * 64 in result.tx_data["data"]
        assert "unlimited" in result.description

    def test_approve_resolver_failure(self, boom_adapter: AaveV3Adapter) -> None:
        result = boom_adapter.build_approve_tx("USDC")
        assert result.success is False


# =============================================================================
# Reserve data cache + isolation mode
# =============================================================================


class TestReserveDataCache:
    def test_get_reserve_data_returns_none_when_missing(
        self, adapter: AaveV3Adapter
    ) -> None:
        assert adapter.get_reserve_data("USDC") is None

    def test_set_then_get_reserve_data(self, adapter: AaveV3Adapter) -> None:
        rd = AaveV3ReserveData(asset="USDC", asset_address=USDC_ADDRESS)
        adapter.set_reserve_data("USDC", rd)
        assert adapter.get_reserve_data("USDC") is rd

    def test_get_isolation_mode_debt_ceiling_zero_when_not_isolated(
        self, adapter: AaveV3Adapter
    ) -> None:
        rd = AaveV3ReserveData(asset="USDC", asset_address=USDC_ADDRESS)
        adapter.set_reserve_data("USDC", rd)
        assert adapter.get_isolation_mode_debt_ceiling("USDC") == Decimal("0")

    def test_get_isolation_mode_debt_ceiling_returns_value_when_isolated(
        self, adapter: AaveV3Adapter
    ) -> None:
        rd = AaveV3ReserveData(
            asset="USDC", asset_address=USDC_ADDRESS, debt_ceiling=Decimal("1000000")
        )
        adapter.set_reserve_data("USDC", rd)
        assert adapter.get_isolation_mode_debt_ceiling("USDC") == Decimal("1000000")

    def test_get_isolation_mode_debt_ceiling_returns_zero_when_unknown_asset(
        self, adapter: AaveV3Adapter
    ) -> None:
        # Unknown asset -> no reserve in cache -> 0 (covers the "or" branch)
        assert adapter.get_isolation_mode_debt_ceiling("UNKNOWN") == Decimal("0")

    def test_is_asset_isolated_true(self, adapter: AaveV3Adapter) -> None:
        rd = AaveV3ReserveData(
            asset="USDC", asset_address=USDC_ADDRESS, debt_ceiling=Decimal("1")
        )
        adapter.set_reserve_data("USDC", rd)
        assert adapter.is_asset_isolated("USDC") is True

    def test_is_asset_isolated_false_when_not_isolated(self, adapter: AaveV3Adapter) -> None:
        rd = AaveV3ReserveData(asset="USDC", asset_address=USDC_ADDRESS)
        adapter.set_reserve_data("USDC", rd)
        assert adapter.is_asset_isolated("USDC") is False

    def test_is_asset_isolated_false_when_unknown(self, adapter: AaveV3Adapter) -> None:
        assert adapter.is_asset_isolated("UNKNOWN") is False


# =============================================================================
# MAX_UINT256 constant sanity (covers a single line for the import path)
# =============================================================================


class TestConstants:
    def test_max_uint256_value(self) -> None:
        assert MAX_UINT256 == 2**256 - 1
