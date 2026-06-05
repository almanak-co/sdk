"""Unit tests for position health monitoring."""

from dataclasses import fields
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.position_health import (
    DeleverageTrigger,
    HealthFactorProvider,
    PositionHealth,
    PositionHealthProvider,
    PTPositionHealth,
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
        assert pt_health.principal_token_market == "0xpendle_market"

    def test_principal_token_market_populates_legacy_alias(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            principal_token_market="0xpt_market",
        )
        assert pt_health.principal_token_market == "0xpt_market"
        assert pt_health.pendle_market == "0xpt_market"

    def test_divergent_principal_token_market_aliases_fail_loudly(self):
        with pytest.raises(ValueError, match="principal_token_market and pendle_market must match"):
            PTPositionHealth(
                health_factor=Decimal("2.0"),
                collateral_value_usd=Decimal("10000"),
                debt_value_usd=Decimal("4575"),
                lltv=Decimal("0.915"),
                principal_token_market="0xpt_market",
                pendle_market="0xother_market",
            )

    def test_legacy_pendle_market_field_order_is_preserved(self):
        names = [field.name for field in fields(PTPositionHealth)]
        assert names[names.index("days_to_maturity") + 1] == "pendle_market"
        assert names.index("pendle_market") < names.index("principal_token_market")

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

    def test_to_dict_includes_principal_token_market_and_legacy_alias(self):
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
        assert d["principal_token_market"] == "0xpendle_market"
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
    """Aave V3: drive the lending-read seam by mocking ``gateway_client.eth_call``.

    VIB-4851 migrated the Aave health read off the in-strategy
    ``Web3(HTTPProvider)`` path onto
    ``read_lending_account_state`` -> the connector-owned
    ``AAVE_FORK_ACCOUNT_STATE_READ`` spec. The reads now resolve the pool address
    through the same ``AddressRegistry`` the intent path uses, and the gateway
    owns the single ``eth_call`` round-trip. These tests mock that round-trip,
    returning the ABI-encoded ``getUserAccountData`` (6 uint256 words) +
    ``getUserEMode`` (1 word) blobs the Aave reducer decodes, in the order the
    spec's ``build_calls`` emits them.
    """

    # Selectors emitted by ``_build_aave_account_state_calls`` (see
    # ``lending_read_base``): getUserAccountData, then getUserEMode.
    _ACCOUNT_DATA_SELECTOR = "0xbf92857c"
    _USER_EMODE_SELECTOR = "0xeddf1b79"

    @staticmethod
    def _encode_account_data(
        collateral_usd: str,
        debt_usd: str,
        liquidation_threshold_bps: int,
        health_factor: str,
    ) -> str:
        """ABI-encode an Aave ``getUserAccountData`` return blob (6 uint256 words).

        Word layout the reducer decodes (``parse_account_state_hex``):
        [0] totalCollateralBase (1e8 USD)
        [1] totalDebtBase (1e8 USD)
        [2] availableBorrowsBase (1e8 USD) -- not decoded
        [3] currentLiquidationThreshold (bps)
        [4] ltv (bps) -- not decoded
        [5] healthFactor (1e18)
        """
        words = [
            int(Decimal(collateral_usd) * Decimal("1e8")),
            int(Decimal(debt_usd) * Decimal("1e8")),
            int(Decimal("2000") * Decimal("1e8")),  # availableBorrowsBase (unused)
            liquidation_threshold_bps,
            8000,  # ltv (unused)
            int(Decimal(health_factor) * Decimal("1e18")),
        ]
        return "0x" + "".join(f"{w:064x}" for w in words)

    @staticmethod
    def _encode_emode(category: int) -> str:
        """ABI-encode an Aave ``getUserEMode`` return blob (1 uint256 word)."""
        return "0x" + f"{category:064x}"

    @classmethod
    def _make_gateway(
        cls,
        *,
        collateral_usd: str,
        debt_usd: str,
        liquidation_threshold_bps: int,
        health_factor: str,
        e_mode_category: int = 0,
    ) -> MagicMock:
        """Build a connected mock gateway whose ``eth_call`` returns the Aave blobs."""
        account_blob = cls._encode_account_data(collateral_usd, debt_usd, liquidation_threshold_bps, health_factor)
        emode_blob = cls._encode_emode(e_mode_category)

        def _eth_call(chain, to, data, block=None):
            selector = data[:10].lower()
            if selector == cls._ACCOUNT_DATA_SELECTOR:
                return account_blob
            if selector == cls._USER_EMODE_SELECTOR:
                return emode_blob
            raise AssertionError(f"unexpected Aave selector {selector}")

        gw = MagicMock()
        gw.is_connected = True
        gw.eth_call.side_effect = _eth_call
        return gw

    def test_aave_hf_returned_from_getuseraccountdata(self):
        # Aave returns healthFactor in 1e18. 1.75e18 -> HF=1.75; collateral 10000,
        # debt 5000 (8-decimal USD base on-chain).
        gw = self._make_gateway(
            collateral_usd="10000",
            debt_usd="5000",
            liquidation_threshold_bps=8500,
            health_factor="1.75",
        )
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        health = provider.get_health("aave_v3", "ethereum_pool", "0xabc")

        assert health.health_factor == Decimal("1.75")
        assert health.protocol == "aave_v3"
        assert health.collateral_value_usd == Decimal("10000")
        assert health.debt_value_usd == Decimal("5000")

    def test_aave_hf_dispatch_via_get_health_factor(self):
        gw = self._make_gateway(
            collateral_usd="10000",
            debt_usd="5000",
            liquidation_threshold_bps=8500,
            health_factor="1.42",
        )
        hf = get_health_factor(
            chain="ethereum",
            protocol="aave_v3",
            wallet="0xabc",
            market="ethereum_pool",
            gateway_client=gw,
        )
        assert hf == Decimal("1.42")

    def test_aave_unsupported_chain_raises(self):
        # An unknown chain has no registry-resolved pool, so the read fails closed
        # (returns None) and the provider raises rather than fabricating a HF.
        gw = self._make_gateway(
            collateral_usd="10000",
            debt_usd="5000",
            liquidation_threshold_bps=8500,
            health_factor="1.75",
        )
        provider = PositionHealthProvider(chain="unknown_chain_x", gateway_client=gw)
        with pytest.raises(ValueError, match="Failed to read aave_v3 account state"):
            provider.get_health("aave_v3", "m", "0xabc")

    def test_aave_missing_gateway_raises(self):
        # VIB-4851: the rpc_url-only Web3(HTTPProvider) path (a gateway-boundary
        # violation) is gone. A missing gateway client must fail closed.
        provider = PositionHealthProvider(chain="ethereum", gateway_client=None)
        with pytest.raises(ValueError, match="GatewayClient is required"):
            provider.get_health("aave_v3", "m", "0xabc")

    def test_aave_hf_supported_on_bsc(self):
        """Regression (ALM-2794): bsc health used to raise "not configured".

        The pre-seam health path hardcoded a 4-chain subset that omitted bsc, so a
        live ``aave_v3`` strategy on bsc got "not configured for chain: bsc" while
        its SUPPLY/BORROW intents executed fine -- tripping HF-safety logic into
        false emergency unwinds. Health now resolves the pool through the same
        ``AddressRegistry`` the intent path uses, so bsc resolves and returns the
        on-chain HF. The stronger invariant is pinned in
        ``test_aave_registry_resolves_pool_for_every_execution_chain``.
        """
        gw = self._make_gateway(
            collateral_usd="10000",
            debt_usd="5000",
            liquidation_threshold_bps=8500,
            health_factor="1.6",
        )
        provider = PositionHealthProvider(chain="bsc", gateway_client=gw)
        health = provider.get_health("aave_v3", "bsc_pool", "0xabc")

        assert health.health_factor == Decimal("1.6")
        assert health.protocol == "aave_v3"

    def test_aave_hf_resolves_bnb_alias_to_bsc(self):
        """Chain aliases must canonicalize before the pool lookup.

        The ``AddressRegistry`` resolves on the canonical name ("bsc"). A caller
        passing the "bnb" alias (as the execution path tolerates) must still
        resolve, not fail closed -- the provider canonicalizes ``self._chain`` at
        construction.
        """
        gw = self._make_gateway(
            collateral_usd="10000",
            debt_usd="5000",
            liquidation_threshold_bps=8500,
            health_factor="1.6",
        )
        provider = PositionHealthProvider(chain="bnb", gateway_client=gw)
        assert provider._chain == "bsc"  # canonicalized at construction
        health = provider.get_health("aave_v3", "bsc_pool", "0xabc")

        assert health.protocol == "aave_v3"

    def test_aave_registry_resolves_pool_for_every_execution_chain(self):
        """Drift guard (stronger than ALM-2794's original): health support must
        never fall behind execution support.

        The original bug was a private copy of the pool-address table that drifted
        behind the connector's. The seam removes the copy entirely: the health read
        resolves the pool via ``LendingReadRegistry.position_manager_address``, the
        same address book the intent path uses. Pin the invariant directly -- every
        chain the connector can execute Aave V3 on (incl. bsc) must resolve a pool
        through the registry. If someone re-introduces a hardcoded subset or breaks
        an address-book entry, this fails closed here.
        """
        from almanak.connectors._strategy_base.lending_read_registry import (
            LendingReadRegistry,
        )
        from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES

        assert "bsc" in AAVE_V3_POOL_ADDRESSES  # the chain that surfaced ALM-2794
        # Assert the EXACT address (not just truthy): the seam contract is "health
        # resolves through the same address book as execution", so a registry that
        # pointed at a different non-empty pool for a chain would still be a drift.
        for chain, expected_pool in AAVE_V3_POOL_ADDRESSES.items():
            assert LendingReadRegistry.position_manager_address("aave_v3", chain) == expected_pool, (
                f"registry resolved the wrong Aave V3 pool for execution chain {chain!r}"
            )


class TestMorphoHealthFactorProvider:
    """Morpho Blue: assert the seam mapping + the price-override translation.

    VIB-4851 migrated Morpho health onto ``read_lending_account_state`` ->
    ``MORPHO_BLUE_ACCOUNT_STATE_READ``. Because Morpho's catalogue / price
    injection is fiddly to encode at the eth_call level, these tests use strategy
    (B): mock ``read_lending_account_state`` to return a crafted
    ``LendingAccountState`` and assert ``_to_position_health`` maps it to the exact
    expected ``PositionHealth``, and unit-test ``_build_price_oracle_dict``'s
    same-asset / cross-asset semantics directly.
    """

    # ``read_lending_account_state`` is imported function-locally inside
    # ``_read_account_state`` from ``lending_reads`` (VIB-4851 PR-2 moved the
    # light reader there; ``lending_accounting`` re-exports it). Patch it at
    # that definition module so the function-local import binds to the mock.
    _SEAM_TARGET = "almanak.framework.accounting.lending_reads.read_lending_account_state"
    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"

    def test_morpho_hf_same_asset_market(self):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        # Same-asset market: prices default to 1. The reducer would value
        # collateral=10, debt=5, lltv=0.915 -> HF=(10*0.915)/5=1.83. We feed that
        # already-reduced state through the seam mock and assert the mapping.
        crafted = LendingAccountState(
            collateral_usd=Decimal("10"),
            debt_usd=Decimal("5"),
            health_factor=Decimal("1.83"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.915"),
        )
        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=crafted) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            health = provider.get_health("morpho_blue", "0xmarket", "0xabc")

        assert health.health_factor == Decimal("1.83")
        assert health.protocol == "morpho_blue"
        assert health.collateral_value_usd == Decimal("10")
        assert health.debt_value_usd == Decimal("5")
        # Same-asset market with no overrides -> {symbol: 1} injected into the seam.
        seam_kwargs = mock_seam.call_args.kwargs
        assert seam_kwargs["price_oracle"] == {"WETH": Decimal("1")}
        assert seam_kwargs["market_id"] == "0xmarket"

    def test_morpho_cross_asset_requires_prices(self):
        cross_asset_params = {
            "collateral_token": "wstETH",
            "loan_token": "USDC",  # cross-asset
            "lltv": int(Decimal("0.86") * Decimal("1e18")),
        }
        with patch(self._MARKET_PARAMS_TARGET, return_value=cross_asset_params):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            # No price override -> must raise to avoid silent miscalculation.
            with pytest.raises(ValueError, match="Price overrides required"):
                provider.get_health("morpho_blue", "0xmarket", "0xabc")

    def test_morpho_dispatch_via_get_health_factor(self):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        # (10 * 0.90) / 5 = 1.80 via the "morpho" alias through get_health_factor.
        crafted = LendingAccountState(
            collateral_usd=Decimal("10"),
            debt_usd=Decimal("5"),
            health_factor=Decimal("1.80"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.90"),
        )
        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.90") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=crafted),
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
        ):
            hf = get_health_factor(
                chain="ethereum",
                protocol="morpho",  # alias
                wallet="0xabc",
                market="0xmarket",
                gateway_client=MagicMock(),
            )
        assert hf == Decimal("1.80")

    def test_build_price_oracle_dict_same_asset_defaults_to_one(self):
        """Same-asset market with no overrides defaults the single token to $1."""
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(
            self._MARKET_PARAMS_TARGET,
            return_value={"collateral_token": "WETH", "loan_token": "WETH"},
        ):
            oracle = provider._build_price_oracle_dict("0xmarket", None, None)
        assert oracle == {"WETH": Decimal("1")}

    def test_build_price_oracle_dict_cross_asset_with_overrides(self):
        """Cross-asset market keys each leg's symbol to its override price."""
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(
            self._MARKET_PARAMS_TARGET,
            return_value={"collateral_token": "wstETH", "loan_token": "USDC"},
        ):
            oracle = provider._build_price_oracle_dict("0xmarket", Decimal("2500"), Decimal("1"))
        assert oracle == {"USDC": Decimal("1"), "wstETH": Decimal("2500")}

    def test_build_price_oracle_dict_off_catalogue_fails_closed(self):
        """An off-catalogue market (no params) fails closed rather than guessing."""
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value=None):
            with pytest.raises(ValueError, match="not found"):
                provider._build_price_oracle_dict("0xnope", None, None)

    def test_build_price_oracle_dict_missing_symbols_fails_closed(self):
        """Market params present but without collateral/loan symbols fails closed.

        A catalogue entry that resolves but omits the token symbols cannot be
        valued -- the seam needs both symbols to price the legs. Empty != Zero:
        raise rather than guess.
        """
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(
            self._MARKET_PARAMS_TARGET,
            return_value={"lltv": 860000000000000000},  # no collateral/loan token
        ):
            with pytest.raises(ValueError, match="no collateral/loan"):
                provider._build_price_oracle_dict("0xmarket", None, None)

    def test_to_position_health_none_state_raises(self):
        """``_to_position_health`` fails closed on a ``None`` state.

        A failed seam read must surface as an error, never a fabricated
        healthy/zero ``PositionHealth`` (Empty != Zero).
        """
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with pytest.raises(ValueError, match="account state is unavailable"):
            provider._to_position_health(None, protocol="morpho_blue", market_id="0xmarket")

    def test_to_position_health_none_hf_with_debt_raises(self):
        """Positive debt but no measured HF must raise, not report Infinity.

        Reporting ``Infinity`` (the no-debt sentinel) for a position that DOES
        carry debt but whose HF could not be measured would mask liquidation
        risk. The adapter fails closed instead (Empty != Zero / raise-on-failure).
        """
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        state = LendingAccountState(
            collateral_usd=Decimal("100"),
            debt_usd=Decimal("50"),  # positive debt
            health_factor=None,  # unmeasured
            liquidation_threshold_bps=8500,
            e_mode_category=0,
            family="aave",
        )
        with pytest.raises(ValueError, match="refusing to fabricate"):
            provider._to_position_health(state, protocol="aave_v3", market_id="m")


class TestPTPositionHealthSeam:
    """``get_pt_position_health`` over the VIB-4851 seam-backed Morpho base.

    The base Morpho health now flows through ``get_health("morpho_blue", ...)``
    (-> ``read_lending_account_state``); the principal-token on-chain enrichment
    (currently Pendle-owned, VIB-4931's territory) is layered on top. These tests
    mock BOTH the seam and the reader registry so the full method body executes,
    proving the repoint preserves the base health fields and composes PT metrics
    onto the ``PTPositionHealth``.
    """

    _SEAM_TARGET = "almanak.framework.accounting.lending_reads.read_lending_account_state"
    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"
    _DEFAULT_PT_READER_TARGET = (
        "almanak.connectors._strategy_principal_token_market_reader_registry."
        "PRINCIPAL_TOKEN_MARKET_READ_REGISTRY.build_default_reader"
    )
    _PT_READER_BY_PROTOCOL_TARGET = (
        "almanak.connectors._strategy_principal_token_market_reader_registry."
        "PRINCIPAL_TOKEN_MARKET_READ_REGISTRY.build_reader"
    )

    @staticmethod
    def _morpho_state():
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        # collateral 10, debt 5, lltv 0.915 -> HF 1.83 (same-asset, prices=1).
        return LendingAccountState(
            collateral_usd=Decimal("10"),
            debt_usd=Decimal("5"),
            health_factor=Decimal("1.83"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.915"),
        )

    def test_pt_health_not_expired_composes_default_reader_metrics(self):
        reader = MagicMock()
        reader.get_implied_apy.return_value = Decimal("0.10")  # 10% APY
        reader.is_market_expired.return_value = False
        reader.get_pt_to_asset_rate.return_value = Decimal("0.98")  # 2% discount

        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._morpho_state()),
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
            patch(self._DEFAULT_PT_READER_TARGET, return_value=reader),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            pt = provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
                user_address="0xabc",
            )

        # Base (seam-derived) health fields preserved.
        assert pt.health_factor == Decimal("1.83")
        assert pt.collateral_value_usd == Decimal("10")
        assert pt.debt_value_usd == Decimal("5")
        assert pt.protocol == "morpho_blue"
        # Principal-token enrichment composed on top.
        assert pt.implied_apy == Decimal("0.10")
        assert pt.pt_discount_pct == (Decimal("1") - Decimal("0.98")) * Decimal("100")
        assert pt.days_to_maturity > 0
        assert pt.principal_token_market == "0xpendle"
        assert pt.pendle_market == "0xpendle"

    def test_pt_health_explicit_protocol_uses_named_reader(self):
        reader = MagicMock()
        reader.get_implied_apy.return_value = Decimal("0.05")
        reader.is_market_expired.return_value = True

        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        gateway_client = MagicMock()
        with (
            patch(self._SEAM_TARGET, return_value=self._morpho_state()),
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
            patch(self._DEFAULT_PT_READER_TARGET) as default_reader,
            patch(self._PT_READER_BY_PROTOCOL_TARGET, return_value=reader) as build_reader,
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=gateway_client)
            pt = provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                principal_token_market_address="0xptmarket",
                principal_token_protocol="pendle",
                user_address="0xabc",
            )

        default_reader.assert_not_called()
        build_reader.assert_called_once()
        assert build_reader.call_args.args == ("pendle",)
        assert build_reader.call_args.kwargs["chain"] == "ethereum"
        assert build_reader.call_args.kwargs["gateway_client"] is gateway_client
        reader.get_implied_apy.assert_called_once_with("0xptmarket")
        assert pt.days_to_maturity == 0
        assert pt.principal_token_market == "0xptmarket"
        assert pt.pendle_market == "0xptmarket"

    def test_pt_health_expired_market_zero_days(self):
        reader = MagicMock()
        reader.get_implied_apy.return_value = Decimal("0.05")
        reader.is_market_expired.return_value = True  # expired branch

        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._morpho_state()),
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
            patch(self._DEFAULT_PT_READER_TARGET, return_value=reader),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            pt = provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
                user_address="0xabc",
            )

        assert pt.days_to_maturity == 0
        assert pt.maturity_risk == "expired"
        # Base health still flows through from the seam.
        assert pt.health_factor == Decimal("1.83")

    def test_pt_health_reader_failure_is_swallowed(self):
        # A principal-token read failure must NOT void the base Morpho health: the
        # enrichment is best-effort (try/except logs a warning), the base HF
        # still surfaces. Guards the "PT health still works" contract.
        reader = MagicMock()
        reader.get_implied_apy.side_effect = RuntimeError("pt reader down")

        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._morpho_state()),
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
            patch(self._DEFAULT_PT_READER_TARGET, return_value=reader),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            pt = provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
                user_address="0xabc",
            )

        # Base health preserved; PT metrics fall back to their defaults.
        assert pt.health_factor == Decimal("1.83")
        assert pt.implied_apy == Decimal("0")
        assert pt.days_to_maturity == 0

    def test_pt_health_unknown_explicit_protocol_fails_loudly(self):
        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._morpho_state()) as seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
            patch(self._PT_READER_BY_PROTOCOL_TARGET) as build_reader,
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            with pytest.raises(ValueError, match="unknown principal_token_protocol 'DOES_NOT_EXIST'"):
                provider.get_pt_position_health(
                    morpho_market_id="0xmarket",
                    principal_token_market_address="0xptmarket",
                    principal_token_protocol="DOES_NOT_EXIST",
                    user_address="0xabc",
                )

        seam.assert_not_called()
        build_reader.assert_not_called()

    def test_pt_health_requires_principal_token_market_address(self):
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with pytest.raises(ValueError, match="principal_token_market_address is required"):
            provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                user_address="0xabc",
            )

    def test_pt_health_requires_user_address(self):
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with pytest.raises(ValueError, match="user_address is required"):
            provider.get_pt_position_health(
                morpho_market_id="0xmarket",
                principal_token_market_address="0xptmarket",
            )


def _pad32(value: int) -> str:
    """Right-pad a uint to a 32-byte (64 hex char) ABI word."""
    return f"{value:064x}"


def _compound_asset_info_blob(*, price_feed: str, scale: int, liquidate_cf: int) -> str:
    """ABI-encode an ``getAssetInfoByAddress`` AssetInfo return blob (8 words).

    Layout the connector decoder (``_parse_asset_info_hex``) reads:
      [0] offset · [1] asset · [2] priceFeed · [3] scale · [4] borrowCF
      [5] liquidateCF · [6] liquidationFactor · [7] supplyCap
    Only priceFeed (word 2), scale (word 3), liquidateCF (word 5) are decoded.
    """
    pf_int = int(price_feed.lower().replace("0x", ""), 16)
    words = [
        _pad32(0),  # offset
        _pad32(int("11" * 20, 16)),  # asset (address)
        _pad32(pf_int),  # priceFeed
        _pad32(scale),  # scale
        _pad32(int(Decimal("0.825") * Decimal("1e18"))),  # borrow_cf (unused)
        _pad32(liquidate_cf),  # liquidate_cf
        _pad32(int(Decimal("0.95") * Decimal("1e18"))),  # liquidation_factor (unused)
        _pad32(0),  # supplyCap (unused)
    ]
    return "0x" + "".join(words)


class TestCompoundHealthFactorProvider:
    """Compound V3: drive the multi-collateral health read by mocking ``gateway_client.eth_call``.

    VIB-4851 PR-2 migrated the Compound health read off the in-strategy
    ``Web3(HTTPProvider)`` path onto the connector-owned, gateway-routed
    ``read_lending_market_health`` ->
    ``read_compound_v3_market_health``. It preserves the product-owner-chosen SUMMED
    health factor ``HF = Σ_held(value × LCF) / debt`` exactly, reading each held
    collateral's price/scale/liquidation-factor ON-CHAIN. These tests mock the single
    gateway ``eth_call`` round-trip, dispatching on the 4-byte selector to return the
    ABI-encoded Comet blobs the connector decodes — mirroring the Aave rewrite above.
    """

    # Selectors the connector read emits (see ``lending_read_base`` PR-2 primitives).
    _COLLATERAL_BALANCE_SELECTOR = "0x5c2549ee"  # collateralBalanceOf(user, asset)
    _ASSET_INFO_SELECTOR = "0x3b3bec2e"  # getAssetInfoByAddress(asset)
    _GET_PRICE_SELECTOR = "0x41976e09"  # getPrice(priceFeed)
    _BORROW_BALANCE_SELECTOR = "0x374c49b4"  # borrowBalanceOf(user)

    # WETH on Ethereum — the only collateral the basic test gives a non-zero balance.
    _WETH_ETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    _PRICE_FEED = "0x" + "fe" * 20  # 0x…feed-style sentinel feed address

    @classmethod
    def _make_gateway(
        cls,
        *,
        borrow_raw: int,
        weth_balance_raw: int = 0,
        weth_price_8dec: int = 2000 * 10**8,
        scale: int = int(Decimal("1e18")),
        liquidate_cf: int = int(Decimal("0.895") * Decimal("1e18")),
    ) -> MagicMock:
        """Build a connected mock gateway whose ``eth_call`` returns the Comet blobs.

        ``collateralBalanceOf`` returns ``weth_balance_raw`` only when the asset arg
        matches WETH's padded address (every other collateral → 0, i.e. skipped).
        """
        asset_info_blob = _compound_asset_info_blob(price_feed=cls._PRICE_FEED, scale=scale, liquidate_cf=liquidate_cf)

        def _eth_call(chain, to, data, block=None):
            selector = data[:10].lower()
            if selector == cls._COLLATERAL_BALANCE_SELECTOR:
                # calldata = selector + pad(user) + pad(asset); asset is word 2.
                asset_word = data[10 + 64 : 10 + 128]
                asset = "0x" + asset_word[24:]
                if asset.lower() == cls._WETH_ETH.lower():
                    return "0x" + _pad32(weth_balance_raw)
                return "0x" + _pad32(0)
            if selector == cls._ASSET_INFO_SELECTOR:
                return asset_info_blob
            if selector == cls._GET_PRICE_SELECTOR:
                return "0x" + _pad32(weth_price_8dec)
            if selector == cls._BORROW_BALANCE_SELECTOR:
                return "0x" + _pad32(borrow_raw)
            raise AssertionError(f"unexpected Compound selector {selector}")

        gw = MagicMock()
        gw.is_connected = True
        gw.eth_call.side_effect = _eth_call
        return gw

    def test_compound_hf_basic(self):
        # Single collateral (WETH 1 token, price $2000, liq_cf=0.895) +
        # borrow 1000 USDC at $1 -> HF = 1*2000*0.895 / 1000 = 1.79
        gw = self._make_gateway(
            borrow_raw=1_000 * 10**6,
            weth_balance_raw=int(Decimal("1") * Decimal("1e18")),
        )
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        health = provider.get_health("compound_v3", "usdc", "0xabc")

        assert health.protocol == "compound_v3"
        # HF = liquidation_threshold / debt = (1 * 2000 * 0.895) / 1000 = 1.79
        assert health.health_factor == Decimal("1.79")
        assert health.collateral_value_usd == Decimal("2000")
        assert health.debt_value_usd == Decimal("1000")

    def test_compound_no_debt_is_infinity(self):
        # Borrow 0, no collateral balances (all skipped) -> HF Infinity, collateral 0.
        gw = self._make_gateway(borrow_raw=0, weth_balance_raw=0)
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        health = provider.get_health("compound_v3", "usdc", "0xabc")

        assert health.health_factor == Decimal("Infinity")
        assert health.collateral_value_usd == Decimal("0")

    def test_compound_unknown_market_raises(self):
        # Connected gateway, but the market id is not in the catalogue -> the registry's
        # market_health_inputs returns None and the provider raises "not found".
        gw = self._make_gateway(borrow_raw=0)
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        with pytest.raises(ValueError, match="not found"):
            provider.get_health("compound_v3", "nonexistent_market_xyz", "0xabc")

    def test_compound_unknown_chain_raises(self):
        gw = self._make_gateway(borrow_raw=0)
        provider = PositionHealthProvider(chain="chain_that_does_not_exist", gateway_client=gw)
        with pytest.raises(ValueError, match="not configured"):
            provider.get_health("compound_v3", "usdc", "0xabc")

    def test_compound_weth_base_requires_price_oracle(self):
        """WETH/AERO (non-stable) Compound base markets MUST fail closed when no
        price_oracle is provided -- silently assuming $1 inflates HF by 1000x+.
        """
        # WETH Comet on Ethereum: base_token='WETH' (not a stablecoin). Borrow > 0 with
        # no oracle -> _resolve_base_price raises. Collaterals all 0 (skipped).
        gw = self._make_gateway(borrow_raw=10 * 10**18, weth_balance_raw=0)
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        with pytest.raises(ValueError, match=r"not a recognized USD stablecoin"):
            provider.get_health("compound_v3", "weth", "0xabc")

    def test_compound_async_price_oracle_path(self):
        """Compound V3 supports the async PriceOracle Protocol for non-stable bases."""

        class _AsyncOracle:
            async def get_aggregated_price(self, token, quote="USD"):
                class _R:
                    price = Decimal("2500")

                return _R()

        gw = self._make_gateway(borrow_raw=1 * 10**18, weth_balance_raw=0)
        provider = PositionHealthProvider(
            chain="ethereum",
            gateway_client=gw,
            price_oracle=_AsyncOracle(),
        )
        health = provider.get_health("compound_v3", "weth", "0xabc")
        # 1 WETH debt * $2500 = $2500 debt value; no collateral -> HF=0.
        assert health.debt_value_usd == Decimal("2500")

    def test_compound_callable_price_oracle_path(self):
        """Compound V3 still accepts a simple callable price_oracle for convenience."""

        def _oracle(symbol):
            return Decimal("1800") if symbol.upper() == "WETH" else Decimal("1")

        gw = self._make_gateway(borrow_raw=2 * 10**18, weth_balance_raw=0)
        provider = PositionHealthProvider(
            chain="ethereum",
            gateway_client=gw,
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
            get_health_factor(chain="ethereum", protocol="unknown_proto", wallet="0xabc", market="m")


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
        inner.get_health.assert_called_once_with(protocol="aave_v3", market_id="m", user_address="0xabc")


class TestVIB4851ReviewFixes:
    """Regression tests for the PR #2597 review findings (Codex / Gemini / CodeRabbit)."""

    _SEAM_TARGET = "almanak.framework.accounting.lending_reads.read_lending_account_state"
    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"

    @staticmethod
    def _aave_state(*, debt, hf, bps=8500):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        return LendingAccountState(
            collateral_usd=Decimal("10000"),
            debt_usd=debt,
            health_factor=hf,
            liquidation_threshold_bps=bps,
            e_mode_category=0,
            lltv=None,
        )

    def test_spark_routes_through_seam_not_unsupported(self):
        # CodeRabbit (major): get_health("spark", ...) used to fall into the
        # unsupported-protocol branch even though Spark is an Aave V3 fork.
        crafted = self._aave_state(debt=Decimal("5000"), hf=Decimal("1.6"))
        with patch(self._SEAM_TARGET, return_value=crafted) as mock_seam:
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            health = provider.get_health("spark", "spark", "0xabc")
        assert health.protocol == "spark"
        assert health.health_factor == Decimal("1.6")
        # Spark is Aave-family: lltv derived from bps (8500 -> 0.85), not state.lltv.
        assert health.lltv == Decimal("0.85")
        assert mock_seam.call_args.kwargs["protocol"] == "spark"

    def test_capped_hf_with_dust_debt_stays_finite(self):
        # Codex (P2) / CodeRabbit (major): a positive (dust) debt whose HF the
        # reducer capped at the 999999 sentinel must NOT be remapped to Infinity --
        # that would make a strategy skip risk/deleverage handling on an open debt.
        crafted = self._aave_state(debt=Decimal("0.01"), hf=Decimal("999999"))
        with patch(self._SEAM_TARGET, return_value=crafted):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            health = provider.get_health("aave_v3", "aave_v3", "0xabc")
        assert health.health_factor == Decimal("999999")
        assert health.health_factor.is_finite()
        assert health.debt_value_usd == Decimal("0.01")

    def test_no_debt_still_maps_to_infinity(self):
        # The genuine no-debt path (debt == 0) must still surface Infinity.
        crafted = self._aave_state(debt=Decimal("0"), hf=Decimal("999999"))
        with patch(self._SEAM_TARGET, return_value=crafted):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            health = provider.get_health("aave_v3", "aave_v3", "0xabc")
        assert health.health_factor == Decimal("Infinity")

    def test_same_asset_lone_debt_override_not_dropped(self):
        # Gemini (high): a lone debt_price_usd override on a same-asset market used
        # to be silently overwritten by the collateral default via a duplicate key.
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        crafted = LendingAccountState(
            collateral_usd=Decimal("10"),
            debt_usd=Decimal("5"),
            health_factor=Decimal("1.83"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.915"),
        )
        same_asset_params = {
            "collateral_token": "WETH",
            "loan_token": "WETH",
            "lltv": int(Decimal("0.915") * Decimal("1e18")),
        }
        with (
            patch(self._SEAM_TARGET, return_value=crafted) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            provider.get_health("morpho_blue", "0xmarket", "0xabc", debt_price_usd=Decimal("2000"))
        # The lone override survives as the single consistent key (not reset to 1).
        assert mock_seam.call_args.kwargs["price_oracle"] == {"WETH": Decimal("2000")}

    def test_aave_none_market_id_read_failure_raises_clean(self):
        # Gemini (medium): market_id=None (Aave whole-account) must not raise a
        # TypeError in the error-message slice when the read returns None.
        with patch(self._SEAM_TARGET, return_value=None):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            with pytest.raises(ValueError, match="Failed to read"):
                provider.get_health("aave_v3", None, "0xabc")
