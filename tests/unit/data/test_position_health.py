"""Unit tests for position health monitoring."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.position_health import (
    DeleverageTrigger,
    HealthFactorProvider,
    PTPositionHealth,
    PositionHealth,
    PositionHealthProvider,
    _normalize_protocol,
    _PositionHealthProviderAdapter,
    get_health_factor,
    register_health_factor_provider,
)


# =========================================================================
# PositionHealth Tests
# =========================================================================


class TestPositionHealth:
    """Test PositionHealth dataclass and properties."""

    def test_healthy_position(self):
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False
        assert health.is_critical is False

    def test_warning_position(self):
        health = PositionHealth(
            health_factor=Decimal("1.3"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("7038"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True
        assert health.is_critical is False

    def test_critical_position(self):
        health = PositionHealth(
            health_factor=Decimal("1.05"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("8714"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True
        assert health.is_critical is True

    def test_liquidatable_position(self):
        health = PositionHealth(
            health_factor=Decimal("0.95"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("9631"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is False
        assert health.is_critical is True

    def test_zero_debt_infinite_hf(self):
        health = PositionHealth(
            health_factor=Decimal("Infinity"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False
        assert health.is_critical is False

    def test_zero_collateral_with_debt(self):
        health = PositionHealth(
            health_factor=Decimal("0"),
            collateral_value_usd=Decimal("0"),
            debt_value_usd=Decimal("1000"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is False
        assert health.is_critical is True

    def test_boundary_hf_exactly_one(self):
        health = PositionHealth(
            health_factor=Decimal("1.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("9150"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True

    def test_boundary_hf_exactly_1_5(self):
        health = PositionHealth(
            health_factor=Decimal("1.5"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6100"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False

    def test_to_dict(self):
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            protocol="morpho_blue",
            market_id="0xmarket",
        )
        d = health.to_dict()
        assert d["health_factor"] == "2.0"
        assert d["protocol"] == "morpho_blue"
        assert d["is_healthy"] is True
        assert d["is_warning"] is False


# =========================================================================
# PTPositionHealth Tests
# =========================================================================


class TestPTPositionHealth:
    """Test PTPositionHealth dataclass and maturity risk."""

    def test_extends_position_health(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            implied_apy=Decimal("0.05"),
            pt_discount_pct=Decimal("3.0"),
            days_to_maturity=90,
            pendle_market="0xpendle_market",
        )
        assert pt_health.is_healthy is True
        assert pt_health.implied_apy == Decimal("0.05")
        assert pt_health.days_to_maturity == 90

    def test_maturity_risk_safe(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=60,
        )
        assert pt_health.maturity_risk == "safe"

    def test_maturity_risk_near(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=15,
        )
        assert pt_health.maturity_risk == "near"

    def test_maturity_risk_imminent(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=5,
        )
        assert pt_health.maturity_risk == "imminent"

    def test_maturity_risk_expired(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=0,
        )
        assert pt_health.maturity_risk == "expired"

    def test_maturity_risk_boundary_7_days(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=7,
        )
        assert pt_health.maturity_risk == "imminent"

    def test_maturity_risk_boundary_30_days(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=30,
        )
        assert pt_health.maturity_risk == "near"

    def test_to_dict_includes_pendle_fields(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            implied_apy=Decimal("0.05"),
            pt_discount_pct=Decimal("3.0"),
            days_to_maturity=90,
            pendle_market="0xpendle_market",
        )
        d = pt_health.to_dict()
        assert d["implied_apy"] == "0.05"
        assert d["pt_discount_pct"] == "3.0"
        assert d["days_to_maturity"] == 90
        assert d["pendle_market"] == "0xpendle_market"
        assert d["maturity_risk"] == "safe"
        # Verify base fields are also present
        assert d["health_factor"] == "2.0"
        assert d["is_healthy"] is True


# =========================================================================
# DeleverageTrigger Tests
# =========================================================================


class TestDeleverageTrigger:
    """Test DeleverageTrigger thresholds."""

    def test_default_thresholds(self):
        trigger = DeleverageTrigger()
        assert trigger.warning_hf == Decimal("1.5")
        assert trigger.critical_hf == Decimal("1.2")
        assert trigger.safe_target_hf == Decimal("2.0")

    def test_should_deleverage_true(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.1"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("8318"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is True

    def test_should_deleverage_false(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.8"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("5083"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is False

    def test_should_warn_true(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.3"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("7038"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is True

    def test_should_warn_false(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is False

    def test_custom_thresholds(self):
        trigger = DeleverageTrigger(
            warning_hf=Decimal("2.0"),
            critical_hf=Decimal("1.5"),
            safe_target_hf=Decimal("3.0"),
        )
        health = PositionHealth(
            health_factor=Decimal("1.6"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("5718"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is True
        assert trigger.should_deleverage(health) is False

    def test_infinite_hf_no_deleverage(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("Infinity"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is False
        assert trigger.should_warn(health) is False


# =========================================================================
# Unified HealthFactorProvider Protocol and get_health_factor() Tests
# =========================================================================


class TestProtocolNormalization:
    """Test protocol name normalization aliases."""

    def test_canonical_names(self):
        assert _normalize_protocol("aave_v3") == "aave_v3"
        assert _normalize_protocol("morpho_blue") == "morpho_blue"
        assert _normalize_protocol("compound_v3") == "compound_v3"

    def test_case_insensitive(self):
        assert _normalize_protocol("AAVE_V3") == "aave_v3"
        assert _normalize_protocol("Morpho_Blue") == "morpho_blue"

    def test_aliases(self):
        assert _normalize_protocol("aave") == "aave_v3"
        assert _normalize_protocol("morpho") == "morpho_blue"
        assert _normalize_protocol("compound") == "compound_v3"
        assert _normalize_protocol("comet") == "compound_v3"

    def test_unknown_passthrough(self):
        # Unknown protocols pass through so callers can register custom providers.
        assert _normalize_protocol("custom-lender") == "custom-lender"


class TestHealthFactorProviderProtocol:
    """Test the HealthFactorProvider Protocol."""

    def test_runtime_checkable(self):
        class MyProvider:
            def get_health_factor(self, wallet: str, market: str) -> Decimal:
                return Decimal("2.0")

        assert isinstance(MyProvider(), HealthFactorProvider)

    def test_missing_method_not_conformant(self):
        class NotAProvider:
            pass

        assert not isinstance(NotAProvider(), HealthFactorProvider)


class TestAaveHealthFactorProvider:
    """Aave V3: mock Pool.getUserAccountData() and assert HF returned correctly."""

    def test_aave_hf_returned_from_getuseraccountdata(self):
        # Aave V3 returns healthFactor in ray (1e18). 1.75e18 -> HF=1.75.
        fake_pool = MagicMock()
        fake_pool.functions.getUserAccountData.return_value.call.return_value = (
            10_000 * 10**8,  # totalCollateralBase (8 decimals)
            5_000 * 10**8,  # totalDebtBase
            2_000 * 10**8,  # availableBorrowsBase
            8500,  # currentLiquidationThreshold (bps)
            8000,  # ltv
            int(Decimal("1.75") * Decimal("1e18")),  # healthFactor
        )

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_pool

        with patch("web3.Web3.HTTPProvider"), patch("web3.Web3", return_value=fake_w3):
            with patch(
                "almanak.framework.data.position_health.Web3", return_value=fake_w3
            ) if False else _fake_web3_patch(fake_w3):
                provider = PositionHealthProvider(rpc_url="http://localhost:8545", chain="ethereum")
                health = provider.get_health("aave_v3", "ethereum_pool", "0xabc")

        assert health.health_factor == Decimal("1.75")
        assert health.protocol == "aave_v3"
        assert health.collateral_value_usd == Decimal("10000")
        assert health.debt_value_usd == Decimal("5000")

    def test_aave_hf_dispatch_via_get_health_factor(self):
        fake_pool = MagicMock()
        fake_pool.functions.getUserAccountData.return_value.call.return_value = (
            10_000 * 10**8,
            5_000 * 10**8,
            2_000 * 10**8,
            8500,
            8000,
            int(Decimal("1.42") * Decimal("1e18")),
        )

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_pool

        with _fake_web3_patch(fake_w3):
            hf = get_health_factor(
                chain="ethereum",
                protocol="aave_v3",
                wallet="0xabc",
                market="ethereum_pool",
                rpc_url="http://localhost:8545",
            )
        assert hf == Decimal("1.42")

    def test_aave_unsupported_chain_raises(self):
        provider = PositionHealthProvider(rpc_url="http://localhost:8545", chain="unknown_chain_x")
        with pytest.raises(ValueError, match="Aave V3 not configured"):
            provider.get_health("aave_v3", "m", "0xabc")


def _fake_web3_patch(fake_w3):
    """Patch ``web3.Web3`` so local imports inside ``position_health`` pick up our mock.

    Returns a context manager. ``Web3(...)`` in ``position_health._get_aave_health`` /
    ``_get_compound_health`` will return ``fake_w3``, and ``Web3.HTTPProvider(...)`` is
    also safely mocked (return value is irrelevant since ``fake_w3`` ignores it).
    """
    import web3

    return patch.object(web3, "Web3", return_value=fake_w3)


class TestMorphoHealthFactorProvider:
    """Morpho Blue: mock the SDK and assert HF math is correct."""

    def test_morpho_hf_same_asset_market(self):
        from almanak.framework.data import position_health as ph_module

        fake_sdk = MagicMock()
        fake_sdk.get_position.return_value = MagicMock(
            collateral=Decimal("10"),
            borrow_shares=Decimal("100"),
        )
        fake_sdk.get_market_params.return_value = MagicMock(
            lltv=int(Decimal("0.915") * Decimal("1e18")),
            collateral_token="0xweth",
            loan_token="0xweth",  # same-asset market
        )
        fake_sdk.get_market_state.return_value = MagicMock(
            total_borrow_assets=Decimal("5"),
            total_borrow_shares=Decimal("100"),
        )

        with patch.object(
            ph_module,
            "_get_morpho_health",
            new=None,  # no-op; we're patching the SDK import inside the method
        ) if False else patch(
            "almanak.connectors.morpho_blue.sdk.MorphoBlueSDK",
            return_value=fake_sdk,
        ):
            provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
            health = provider.get_health("morpho_blue", "0xmarket", "0xabc")

        # Same-asset market: prices default to 1.
        # Collateral=10, debt_amount = 100 * 5/100 = 5, lltv=0.915
        # HF = (10 * 0.915) / 5 = 1.83
        assert health.health_factor == Decimal("1.83")
        assert health.protocol == "morpho_blue"
        assert health.collateral_value_usd == Decimal("10")
        assert health.debt_value_usd == Decimal("5")

    def test_morpho_cross_asset_requires_prices(self):
        fake_sdk = MagicMock()
        fake_sdk.get_position.return_value = MagicMock(
            collateral=Decimal("10"),
            borrow_shares=Decimal("100"),
        )
        fake_sdk.get_market_params.return_value = MagicMock(
            lltv=int(Decimal("0.86") * Decimal("1e18")),
            collateral_token="0xweth",
            loan_token="0xusdc",  # cross-asset
        )
        fake_sdk.get_market_state.return_value = MagicMock(
            total_borrow_assets=Decimal("1000"),
            total_borrow_shares=Decimal("100"),
        )

        with patch(
            "almanak.connectors.morpho_blue.sdk.MorphoBlueSDK",
            return_value=fake_sdk,
        ):
            provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
            # No price override -> must raise to avoid silent miscalculation.
            with pytest.raises(ValueError, match="Price overrides required"):
                provider.get_health("morpho_blue", "0xmarket", "0xabc")

    def test_morpho_dispatch_via_get_health_factor(self):
        fake_sdk = MagicMock()
        fake_sdk.get_position.return_value = MagicMock(
            collateral=Decimal("10"),
            borrow_shares=Decimal("100"),
        )
        fake_sdk.get_market_params.return_value = MagicMock(
            lltv=int(Decimal("0.90") * Decimal("1e18")),
            collateral_token="0xweth",
            loan_token="0xweth",
        )
        fake_sdk.get_market_state.return_value = MagicMock(
            total_borrow_assets=Decimal("5"),
            total_borrow_shares=Decimal("100"),
        )

        with patch(
            "almanak.connectors.morpho_blue.sdk.MorphoBlueSDK",
            return_value=fake_sdk,
        ):
            hf = get_health_factor(
                chain="ethereum",
                protocol="morpho",  # alias
                wallet="0xabc",
                market="0xmarket",
                rpc_url="http://x",
            )
        # (10 * 0.90) / 5 = 1.80
        assert hf == Decimal("1.80")


class TestCompoundHealthFactorProvider:
    """Compound V3: mock Comet calls and assert HF math."""

    def test_compound_hf_basic(self):
        # Single collateral (WETH 1 token, price $2000, liq_cf=0.895) +
        # borrow 1000 USDC at $1 -> HF = 1*2000*0.895 / 1000 = 1.79
        fake_comet = MagicMock()
        fake_comet.functions.borrowBalanceOf.return_value.call.return_value = 1_000 * 10**6

        def _collateral_balance_of(user, asset):
            m = MagicMock()
            if asset.lower() == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
                m.call.return_value = int(Decimal("1") * Decimal("1e18"))
            else:
                m.call.return_value = 0
            return m

        fake_comet.functions.collateralBalanceOf.side_effect = _collateral_balance_of

        def _get_asset_info(addr):
            m = MagicMock()
            m.call.return_value = (
                0,  # offset
                addr,  # asset
                "0xpricefeed",  # priceFeed
                int(Decimal("1e18")),  # scale (WETH)
                int(Decimal("0.825") * Decimal("1e18")),  # borrow_cf
                int(Decimal("0.895") * Decimal("1e18")),  # liquidate_cf
                int(Decimal("0.95") * Decimal("1e18")),  # liquidation_factor
                0,  # supplyCap
            )
            return m

        fake_comet.functions.getAssetInfoByAddress.side_effect = _get_asset_info
        fake_comet.functions.getPrice.return_value.call.return_value = 2000 * 10**8

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_comet

        with _fake_web3_patch(fake_w3):
            provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
            health = provider.get_health("compound_v3", "usdc", "0xabc")

        assert health.protocol == "compound_v3"
        # HF = liquidation_threshold / debt = (1 * 2000 * 0.895) / 1000 = 1.79
        assert health.health_factor == Decimal("1.79")
        assert health.collateral_value_usd == Decimal("2000")
        assert health.debt_value_usd == Decimal("1000")

    def test_compound_no_debt_is_infinity(self):
        fake_comet = MagicMock()
        fake_comet.functions.borrowBalanceOf.return_value.call.return_value = 0
        fake_comet.functions.collateralBalanceOf.return_value.call.return_value = 0

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_comet

        with _fake_web3_patch(fake_w3):
            provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
            health = provider.get_health("compound_v3", "usdc", "0xabc")

        assert health.health_factor == Decimal("Infinity")

    def test_compound_unknown_market_raises(self):
        provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
        with pytest.raises(ValueError, match="not found"):
            provider.get_health("compound_v3", "nonexistent_market_xyz", "0xabc")

    def test_compound_unknown_chain_raises(self):
        provider = PositionHealthProvider(rpc_url="http://x", chain="chain_that_does_not_exist")
        with pytest.raises(ValueError, match="not configured"):
            provider.get_health("compound_v3", "usdc", "0xabc")

    def test_compound_weth_base_requires_price_oracle(self):
        """WETH/AERO (non-stable) Compound base markets MUST fail closed when no
        price_oracle is provided -- silently assuming $1 inflates HF by 1000x+.
        """
        fake_comet = MagicMock()
        fake_comet.functions.borrowBalanceOf.return_value.call.return_value = 10 * 10**18
        fake_comet.functions.collateralBalanceOf.return_value.call.return_value = 0

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_comet

        with _fake_web3_patch(fake_w3):
            # WETH Comet on Ethereum: base_token='WETH' (not a stablecoin).
            provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
            with pytest.raises(ValueError, match=r"not a recognized USD stablecoin"):
                provider.get_health("compound_v3", "weth", "0xabc")

    def test_compound_async_price_oracle_path(self):
        """Compound V3 supports the async PriceOracle Protocol for non-stable bases."""

        class _AsyncOracle:
            async def get_aggregated_price(self, token, quote="USD"):
                class _R:
                    price = Decimal("2500")

                return _R()

        fake_comet = MagicMock()
        fake_comet.functions.borrowBalanceOf.return_value.call.return_value = 1 * 10**18
        fake_comet.functions.collateralBalanceOf.return_value.call.return_value = 0

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_comet

        with _fake_web3_patch(fake_w3):
            provider = PositionHealthProvider(
                rpc_url="http://x",
                chain="ethereum",
                price_oracle=_AsyncOracle(),
            )
            health = provider.get_health("compound_v3", "weth", "0xabc")
        # 1 WETH debt * $2500 = $2500 debt value; no collateral -> HF=0.
        assert health.debt_value_usd == Decimal("2500")

    def test_compound_callable_price_oracle_path(self):
        """Compound V3 still accepts a simple callable price_oracle for convenience."""
        fake_comet = MagicMock()
        fake_comet.functions.borrowBalanceOf.return_value.call.return_value = 2 * 10**18
        fake_comet.functions.collateralBalanceOf.return_value.call.return_value = 0

        fake_w3 = MagicMock()
        fake_w3.to_checksum_address.side_effect = lambda a: a
        fake_w3.eth.contract.return_value = fake_comet

        def _oracle(symbol):
            return Decimal("1800") if symbol.upper() == "WETH" else Decimal("1")

        with _fake_web3_patch(fake_w3):
            provider = PositionHealthProvider(
                rpc_url="http://x",
                chain="ethereum",
                price_oracle=_oracle,
            )
            health = provider.get_health("compound_v3", "weth", "0xabc")
        # 2 WETH debt * $1800 = $3600 debt value.
        assert health.debt_value_usd == Decimal("3600")


class TestUnsupportedProtocol:
    """Unknown protocol must raise, not silently return a default HF."""

    def test_unsupported_protocol(self):
        provider = PositionHealthProvider(rpc_url="http://x", chain="ethereum")
        with pytest.raises(ValueError, match="Unsupported protocol"):
            provider.get_health("unknown_proto", "m", "0xabc")

    def test_unsupported_via_dispatch(self):
        with pytest.raises(ValueError, match="Unsupported protocol"):
            get_health_factor(
                chain="ethereum", protocol="unknown_proto", wallet="0xabc", market="m"
            )


class TestProviderRegistry:
    """Users can register custom protocol providers without forking."""

    def test_register_custom_provider(self):
        calls = []

        class _FakeProvider:
            def __init__(self, **kw):
                calls.append(kw)

            def get_health_factor(self, wallet, market):
                return Decimal("3.14")

        register_health_factor_provider("my_custom_lender", _FakeProvider)
        try:
            hf = get_health_factor(
                chain="ethereum",
                protocol="my_custom_lender",
                wallet="0xabc",
                market="m",
            )
        finally:
            # Clean up the registry so other tests see a pristine state.
            from almanak.framework.data.position_health import _HF_FACTORIES

            _HF_FACTORIES.pop("my_custom_lender", None)
        assert hf == Decimal("3.14")
        assert len(calls) == 1

    def test_adapter_wraps_provider_by_protocol(self):
        inner = MagicMock()
        inner.get_health.return_value = PositionHealth(
            health_factor=Decimal("2.5"),
            collateral_value_usd=Decimal("1000"),
            debt_value_usd=Decimal("400"),
            lltv=Decimal("0.8"),
        )
        adapter = _PositionHealthProviderAdapter(inner, "aave_v3")
        hf = adapter.get_health_factor("0xabc", "m")
        assert hf == Decimal("2.5")
        inner.get_health.assert_called_once_with(
            protocol="aave_v3", market_id="m", user_address="0xabc"
        )
