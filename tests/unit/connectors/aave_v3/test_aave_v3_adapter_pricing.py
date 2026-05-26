"""Tests for AaveV3Adapter health-factor + pricing branches.

Covers:
- AaveV3Config validation (zero/negative slippage, address shapes)
- Adapter init: requires price_oracle when allow_placeholder_prices=False
- _default_price_oracle hits known + uppercase fallback + raises on unknown
- create_adapter_with_prices + create_adapter_from_price_oracle_dict
- price oracle from dict raises KeyError for missing token
- calculate_health_factor: empty list, no-debt branch (HF=999999), single
  position liquidation_price branch, missing price warning, missing reserve
  warning, e-mode override branch, non-collateral position skipped
- calculate_liquidation_price: zero-collateral / zero-debt fast path
- calculate_max_borrow: clamps at zero
- calculate_health_factor_after_borrow: zero-debt branch
- AaveV3ReserveData.is_isolated, AaveV3UserAccountData properties + to_dict
- AaveV3Position properties + to_dict
- AaveV3FlashLoanParams.__post_init__ validation
- AaveV3HealthFactorCalculation.is_healthy / buffer_to_liquidation / to_dict
- TransactionResult.to_dict
"""

from __future__ import annotations

import warnings
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aave_v3.adapter import (
    AaveV3Adapter,
    AaveV3Config,
    AaveV3FlashLoanParams,
    AaveV3HealthFactorCalculation,
    AaveV3Position,
    AaveV3ReserveData,
    AaveV3UserAccountData,
    TransactionResult,
    create_adapter_from_price_oracle_dict,
    create_adapter_with_prices,
)


TEST_WALLET = "0x1234567890123456789012345678901234567890"


# =============================================================================
# AaveV3Config validation
# =============================================================================


class TestConfigValidation:
    def test_invalid_chain_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid chain"):
            AaveV3Config(chain="solana", wallet_address=TEST_WALLET)

    def test_invalid_wallet_no_0x(self) -> None:
        with pytest.raises(ValueError, match="Invalid wallet address"):
            AaveV3Config(chain="arbitrum", wallet_address="1234")

    def test_invalid_wallet_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="Invalid wallet address"):
            AaveV3Config(chain="arbitrum", wallet_address="0xabc")

    def test_invalid_slippage_negative(self) -> None:
        with pytest.raises(ValueError, match="Invalid slippage"):
            AaveV3Config(
                chain="arbitrum",
                wallet_address=TEST_WALLET,
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_above_10000(self) -> None:
        with pytest.raises(ValueError, match="Invalid slippage"):
            AaveV3Config(
                chain="arbitrum",
                wallet_address=TEST_WALLET,
                default_slippage_bps=10001,
            )

    def test_placeholder_prices_emits_warning(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            AaveV3Config(
                chain="arbitrum",
                wallet_address=TEST_WALLET,
                allow_placeholder_prices=True,
            )
        assert any("UNSAFE" in str(w.message) for w in caught)


# =============================================================================
# Adapter init / oracle requirements
# =============================================================================


class TestAdapterInit:
    def test_init_without_oracle_and_no_placeholder_raises(self) -> None:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=False,
        )
        with pytest.raises(ValueError, match="requires a price_oracle"):
            AaveV3Adapter(config)

    def test_init_with_oracle_skips_placeholder_branch(self) -> None:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=False,
        )
        oracle = lambda asset: Decimal("100")  # noqa: E731
        adapter = AaveV3Adapter(config, price_oracle=oracle, token_resolver=MagicMock())
        assert adapter._using_placeholder_prices is False

    def test_init_with_placeholder_oracle_logs_warning(self) -> None:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
        )
        adapter = AaveV3Adapter(config, token_resolver=MagicMock())
        assert adapter._using_placeholder_prices is True


# =============================================================================
# _default_price_oracle
# =============================================================================


class TestDefaultPriceOracle:
    @pytest.fixture
    def adapter(self) -> AaveV3Adapter:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
        )
        return AaveV3Adapter(config, token_resolver=MagicMock())

    def test_known_symbol(self, adapter: AaveV3Adapter) -> None:
        assert adapter._default_price_oracle("USDC") == Decimal("1")

    def test_uppercase_fallback(self, adapter: AaveV3Adapter) -> None:
        # 'weth' (lower) not in dict, but 'WETH' is — _default_price_oracle
        # tries asset.upper() as a fallback.
        assert adapter._default_price_oracle("weth") == Decimal("2000")

    def test_unknown_symbol_raises(self, adapter: AaveV3Adapter) -> None:
        with pytest.raises(ValueError, match="No placeholder price"):
            adapter._default_price_oracle("ZZZZZZ")


# =============================================================================
# Factory functions
# =============================================================================


class TestFactories:
    def test_create_adapter_with_prices_lookup(self) -> None:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
        )
        adapter = create_adapter_with_prices(
            config,
            {"WETH": Decimal("3100"), "USDC": Decimal("1.0")},
        )
        assert adapter._using_placeholder_prices is False
        # Hits 'asset in prices' first branch
        assert adapter._price_oracle("WETH") == Decimal("3100")

    def test_create_adapter_with_prices_uppercase_match(self) -> None:
        config = AaveV3Config(chain="arbitrum", wallet_address=TEST_WALLET)
        adapter = create_adapter_with_prices(config, {"WETH": Decimal("3000")})
        # 'weth' lower-case → upper match
        assert adapter._price_oracle("weth") == Decimal("3000")

    def test_create_adapter_with_prices_lowercase_match(self) -> None:
        config = AaveV3Config(chain="arbitrum", wallet_address=TEST_WALLET)
        # 'weth' (lower) is in dict → first uppercase branch fails for 'WETH'
        # then variations loop matches 'WETH'.lower() == 'weth'
        adapter = create_adapter_with_prices(config, {"weth": Decimal("3000")})
        assert adapter._price_oracle("WETH") == Decimal("3000")

    def test_create_adapter_with_prices_missing_raises(self) -> None:
        config = AaveV3Config(chain="arbitrum", wallet_address=TEST_WALLET)
        adapter = create_adapter_with_prices(config, {"USDC": Decimal("1")})
        with pytest.raises(KeyError, match="No price found"):
            adapter._price_oracle("UNKNOWN_TOKEN")

    def test_create_adapter_from_price_oracle_dict(self) -> None:
        adapter = create_adapter_from_price_oracle_dict(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            price_oracle_dict={"WETH": Decimal("3100")},
        )
        assert adapter.chain == "arbitrum"
        assert adapter._price_oracle("WETH") == Decimal("3100")


# =============================================================================
# Health-factor calculation branches
# =============================================================================


@pytest.fixture
def adapter() -> AaveV3Adapter:
    config = AaveV3Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )
    return AaveV3Adapter(config, token_resolver=MagicMock())


class TestHealthFactorCalc:
    def test_empty_positions_returns_no_debt_infinity(self, adapter: AaveV3Adapter) -> None:
        result = adapter.calculate_health_factor([], {}, prices={})
        assert result.health_factor == Decimal("999999")
        assert result.total_collateral_usd == Decimal("0")
        assert result.total_debt_usd == Decimal("0")

    def test_skips_position_when_price_zero(self, adapter: AaveV3Adapter) -> None:
        position = AaveV3Position(
            asset="ZZZ",
            asset_address="0x" + "00" * 20,
            current_atoken_balance=Decimal("100"),
            usage_as_collateral_enabled=True,
        )
        # Override oracle to return 0 (price <= 0 branch)
        adapter._price_oracle = lambda _asset: Decimal("0")
        result = adapter.calculate_health_factor([position], {}, prices={})
        # Position skipped because price <= 0
        assert result.total_collateral_usd == Decimal("0")
        assert "ZZZ" not in result.assets_breakdown

    def test_uses_default_reserve_when_missing(self, adapter: AaveV3Adapter) -> None:
        position = AaveV3Position(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("100"),
            usage_as_collateral_enabled=True,
        )
        # No reserve_data provided → falls back to AaveV3ReserveData defaults
        result = adapter.calculate_health_factor(
            [position], {}, prices={"USDC": Decimal("1")}
        )
        assert "USDC" in result.assets_breakdown

    def test_emode_overrides_liquidation_threshold(self, adapter: AaveV3Adapter) -> None:
        position = AaveV3Position(
            asset="WETH",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("1"),
            current_variable_debt=Decimal("0"),
            usage_as_collateral_enabled=True,
        )
        reserve = AaveV3ReserveData(
            asset="WETH",
            asset_address="0x" + "11" * 20,
            liquidation_threshold=8500,
            emode_liquidation_threshold=9500,
            emode_category=1,
        )
        result = adapter.calculate_health_factor(
            [position],
            {"WETH": reserve},
            prices={"WETH": Decimal("3000")},
            emode_category=1,
        )
        # Stored breakdown LT comes from the e-mode override
        assert result.assets_breakdown["WETH"]["liquidation_threshold_bps"] == 9500

    def test_non_collateral_position_does_not_count_collateral(
        self, adapter: AaveV3Adapter
    ) -> None:
        position = AaveV3Position(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("100"),
            usage_as_collateral_enabled=False,  # NOT collateral
        )
        result = adapter.calculate_health_factor(
            [position], {}, prices={"USDC": Decimal("1")}
        )
        # Skipped from collateral sum even though there's a balance
        assert result.total_collateral_usd == Decimal("0")

    def test_single_position_with_debt_emits_liquidation_price(
        self, adapter: AaveV3Adapter
    ) -> None:
        # single position with both collateral and debt → liquidation_price branch
        position = AaveV3Position(
            asset="WETH",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("1"),
            current_variable_debt=Decimal("0.5"),
            usage_as_collateral_enabled=True,
        )
        reserve = AaveV3ReserveData(
            asset="WETH",
            asset_address="0x" + "11" * 20,
            liquidation_threshold=8500,
        )
        result = adapter.calculate_health_factor(
            [position], {"WETH": reserve}, prices={"WETH": Decimal("3000")}
        )
        assert result.liquidation_price is not None
        assert result.liquidation_price > 0

    def test_single_position_falls_back_to_default_lt_when_reserve_missing(
        self, adapter: AaveV3Adapter
    ) -> None:
        # single-position liquidation_price branch where reserve is None → uses 8000
        position = AaveV3Position(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("1000"),
            current_variable_debt=Decimal("100"),
            usage_as_collateral_enabled=True,
        )
        result = adapter.calculate_health_factor(
            [position], {}, prices={"USDC": Decimal("1")}
        )
        assert result.liquidation_price is not None

    def test_calculate_liquidation_price_zero_collateral(
        self, adapter: AaveV3Adapter
    ) -> None:
        assert (
            adapter.calculate_liquidation_price(
                collateral_asset="USDC",
                collateral_amount=Decimal("0"),
                debt_usd=Decimal("100"),
                liquidation_threshold_bps=8000,
            )
            == Decimal("0")
        )

    def test_calculate_liquidation_price_zero_debt(self, adapter: AaveV3Adapter) -> None:
        assert (
            adapter.calculate_liquidation_price(
                collateral_asset="USDC",
                collateral_amount=Decimal("100"),
                debt_usd=Decimal("0"),
                liquidation_threshold_bps=8000,
            )
            == Decimal("0")
        )

    def test_calculate_liquidation_price_normal(self, adapter: AaveV3Adapter) -> None:
        # debt=80, collateral=1, lt=80% → price = 80 / (1 * 0.8) = 100
        out = adapter.calculate_liquidation_price(
            collateral_asset="WETH",
            collateral_amount=Decimal("1"),
            debt_usd=Decimal("80"),
            liquidation_threshold_bps=8000,
        )
        assert out == Decimal("100")

    def test_calculate_max_borrow_clamps_at_zero(self, adapter: AaveV3Adapter) -> None:
        # current_debt > collateral*ltv → return 0
        out = adapter.calculate_max_borrow(
            collateral_value_usd=Decimal("100"),
            current_debt_usd=Decimal("999"),
            ltv_bps=8000,
        )
        assert out == Decimal("0")

    def test_calculate_max_borrow_normal(self, adapter: AaveV3Adapter) -> None:
        out = adapter.calculate_max_borrow(
            collateral_value_usd=Decimal("1000"),
            current_debt_usd=Decimal("200"),
            ltv_bps=8000,
        )
        assert out == Decimal("600")

    def test_calculate_health_factor_after_borrow_zero_debt_returns_infinity(
        self, adapter: AaveV3Adapter
    ) -> None:
        hf_calc = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("0"),
            total_debt_usd=Decimal("0"),
            weighted_liquidation_threshold=Decimal("0"),
            health_factor=Decimal("999999"),
        )
        out = adapter.calculate_health_factor_after_borrow(hf_calc, Decimal("0"))
        assert out == Decimal("999999")

    def test_calculate_health_factor_after_borrow_normal(
        self, adapter: AaveV3Adapter
    ) -> None:
        hf_calc = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("1000"),
            total_debt_usd=Decimal("100"),
            weighted_liquidation_threshold=Decimal("0.85"),
            health_factor=Decimal("8.5"),
        )
        # new_debt = 100 + 100 = 200; HF = 1000 * 0.85 / 200 = 4.25
        assert adapter.calculate_health_factor_after_borrow(
            hf_calc, Decimal("100")
        ) == Decimal("4.25")


# =============================================================================
# Dataclass property + to_dict coverage
# =============================================================================


class TestAaveV3ReserveData:
    def test_is_isolated_when_debt_ceiling_positive(self) -> None:
        rd = AaveV3ReserveData(
            asset="USDC", asset_address="0x" + "11" * 20, debt_ceiling=Decimal("1000")
        )
        assert rd.is_isolated is True

    def test_to_dict_round_trip(self) -> None:
        rd = AaveV3ReserveData(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            ltv=8000,
            debt_ceiling=Decimal("1"),
        )
        d = rd.to_dict()
        assert d["asset"] == "USDC"
        assert d["ltv"] == 8000
        assert d["is_isolated"] is True


class TestAaveV3UserAccountData:
    def test_health_factor_normalized(self) -> None:
        u = AaveV3UserAccountData(
            total_collateral_base=Decimal("1000"),
            total_debt_base=Decimal("100"),
            available_borrows_base=Decimal("700"),
            current_liquidation_threshold=8500,
            ltv=8000,
            health_factor=Decimal("2") * Decimal("1e18"),
        )
        assert u.health_factor_normalized == Decimal("2")
        assert u.is_liquidatable is False
        # distance_to_liquidation = (2 - 1) / 2 = 0.5
        assert u.distance_to_liquidation == Decimal("0.5")

    def test_is_liquidatable_when_under_one(self) -> None:
        u = AaveV3UserAccountData(
            total_collateral_base=Decimal("100"),
            total_debt_base=Decimal("100"),
            available_borrows_base=Decimal("0"),
            current_liquidation_threshold=8500,
            ltv=8000,
            health_factor=Decimal("0.5") * Decimal("1e18"),
        )
        assert u.is_liquidatable is True
        # distance_to_liquidation: hf < 1 returns 0
        assert u.distance_to_liquidation == Decimal("0")

    def test_distance_to_liquidation_zero_health(self) -> None:
        u = AaveV3UserAccountData(
            total_collateral_base=Decimal("0"),
            total_debt_base=Decimal("100"),
            available_borrows_base=Decimal("0"),
            current_liquidation_threshold=0,
            ltv=0,
            health_factor=Decimal("0"),
        )
        assert u.distance_to_liquidation == Decimal("0")

    def test_to_dict(self) -> None:
        u = AaveV3UserAccountData(
            total_collateral_base=Decimal("1000"),
            total_debt_base=Decimal("100"),
            available_borrows_base=Decimal("700"),
            current_liquidation_threshold=8500,
            ltv=8000,
            health_factor=Decimal("2") * Decimal("1e18"),
        )
        d = u.to_dict()
        assert d["ltv"] == 8000
        assert "health_factor_normalized" in d


class TestAaveV3Position:
    def test_properties(self) -> None:
        p = AaveV3Position(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("100"),
            current_stable_debt=Decimal("10"),
            current_variable_debt=Decimal("5"),
            usage_as_collateral_enabled=True,
        )
        assert p.is_collateral is True
        assert p.total_debt == Decimal("15")
        assert p.has_supply is True
        assert p.has_debt is True

    def test_no_supply_no_debt(self) -> None:
        p = AaveV3Position(asset="USDC", asset_address="0x" + "11" * 20)
        assert p.has_supply is False
        assert p.has_debt is False

    def test_to_dict(self) -> None:
        p = AaveV3Position(
            asset="USDC",
            asset_address="0x" + "11" * 20,
            current_atoken_balance=Decimal("1"),
        )
        d = p.to_dict()
        assert d["asset"] == "USDC"
        assert d["has_supply"] is True


class TestFlashLoanParams:
    def test_validates_assets_amounts_length(self) -> None:
        with pytest.raises(ValueError, match="amounts must have same length"):
            AaveV3FlashLoanParams(
                assets=["a", "b"],
                amounts=[Decimal("1")],
                modes=[0, 0],
                on_behalf_of="0x" + "11" * 20,
            )

    def test_validates_assets_modes_length(self) -> None:
        with pytest.raises(ValueError, match="modes must have same length"):
            AaveV3FlashLoanParams(
                assets=["a"],
                amounts=[Decimal("1")],
                modes=[0, 0],
                on_behalf_of="0x" + "11" * 20,
            )

    def test_rejects_invalid_mode(self) -> None:
        with pytest.raises(ValueError, match="Invalid mode"):
            AaveV3FlashLoanParams(
                assets=["a"],
                amounts=[Decimal("1")],
                modes=[3],
                on_behalf_of="0x" + "11" * 20,
            )

    def test_accepts_valid_modes(self) -> None:
        # No raise
        AaveV3FlashLoanParams(
            assets=["a", "b", "c"],
            amounts=[Decimal("1")] * 3,
            modes=[0, 1, 2],
            on_behalf_of="0x" + "11" * 20,
        )


class TestHealthFactorCalculation:
    def test_is_healthy_true(self) -> None:
        c = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("100"),
            total_debt_usd=Decimal("50"),
            weighted_liquidation_threshold=Decimal("0.85"),
            health_factor=Decimal("1.5"),
        )
        assert c.is_healthy is True
        # buffer_to_liquidation = (1.5 - 1) * 100 = 50
        assert c.buffer_to_liquidation == Decimal("50")

    def test_buffer_to_liquidation_zero_health(self) -> None:
        c = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("0"),
            total_debt_usd=Decimal("0"),
            weighted_liquidation_threshold=Decimal("0"),
            health_factor=Decimal("0"),
        )
        assert c.buffer_to_liquidation == Decimal("0")

    def test_to_dict_handles_liquidation_price_none(self) -> None:
        c = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("100"),
            total_debt_usd=Decimal("50"),
            weighted_liquidation_threshold=Decimal("0.85"),
            health_factor=Decimal("1.5"),
        )
        d = c.to_dict()
        assert d["liquidation_price"] is None

    def test_to_dict_includes_liquidation_price(self) -> None:
        c = AaveV3HealthFactorCalculation(
            total_collateral_usd=Decimal("100"),
            total_debt_usd=Decimal("50"),
            weighted_liquidation_threshold=Decimal("0.85"),
            health_factor=Decimal("1.5"),
            liquidation_price=Decimal("123.45"),
        )
        d = c.to_dict()
        assert d["liquidation_price"] == "123.45"


class TestTransactionResult:
    def test_to_dict_success(self) -> None:
        r = TransactionResult(
            success=True,
            tx_data={"to": "0x" + "11" * 20, "value": 0, "data": "0xab"},
            gas_estimate=100000,
            description="hello",
        )
        d = r.to_dict()
        assert d["success"] is True
        assert d["gas_estimate"] == 100000

    def test_to_dict_failure(self) -> None:
        r = TransactionResult(success=False, error="oops")
        d = r.to_dict()
        assert d["success"] is False
        assert d["error"] == "oops"


# =============================================================================
# _resolve_asset error wrapping (TokenResolutionError) — exercise the import
# =============================================================================


class TestResolveAssetErrorWrapping:
    def test_resolve_asset_wraps_token_resolution_error(self) -> None:
        from almanak.framework.data.tokens.exceptions import TokenResolutionError

        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
        )
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="ZZZ",
            chain="arbitrum",
            reason="not found",
            suggestions=("USDC",),
        )
        adapter = AaveV3Adapter(config, token_resolver=resolver)
        with pytest.raises(TokenResolutionError) as exc:
            adapter._resolve_asset("ZZZ")
        assert "ZZZ" in str(exc.value)
        assert "AaveV3Adapter" in exc.value.reason

    def test_get_decimals_wraps_token_resolution_error(self) -> None:
        from almanak.framework.data.tokens.exceptions import TokenResolutionError

        config = AaveV3Config(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
        )
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="ZZZ", chain="arbitrum", reason="x"
        )
        adapter = AaveV3Adapter(config, token_resolver=resolver)
        with pytest.raises(TokenResolutionError) as exc:
            adapter._get_decimals("ZZZ")
        assert "AaveV3Adapter" in exc.value.reason
