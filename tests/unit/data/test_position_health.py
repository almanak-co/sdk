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
        # Unknown protocols pass through in registry-folded form (case +
        # hyphens folded, B3) so callers can register custom providers.
        # Registration and dispatch fold identically, so the round-trip holds
        # for any spelling of the same name.
        assert _normalize_protocol("custom_lender") == "custom_lender"
        assert _normalize_protocol("custom-lender") == "custom_lender"
        assert _normalize_protocol("Custom-Lender") == "custom_lender"


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
        from almanak.framework.data.position_health import PRICE_SOURCE_SAME_ASSET_UNIT

        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(
            self._MARKET_PARAMS_TARGET,
            return_value={"collateral_token": "WETH", "loan_token": "WETH"},
        ):
            oracle, source = provider._build_price_oracle_dict("morpho_blue", "0xmarket", None, None)
        assert oracle == {"WETH": Decimal("1")}
        assert source == PRICE_SOURCE_SAME_ASSET_UNIT

    def test_build_price_oracle_dict_cross_asset_with_overrides(self):
        """Cross-asset market keys each leg's symbol to its override price."""
        from almanak.framework.data.position_health import PRICE_SOURCE_OVERRIDE

        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(
            self._MARKET_PARAMS_TARGET,
            return_value={"collateral_token": "wstETH", "loan_token": "USDC"},
        ):
            oracle, source = provider._build_price_oracle_dict("morpho_blue", "0xmarket", Decimal("2500"), Decimal("1"))
        assert oracle == {"USDC": Decimal("1"), "wstETH": Decimal("2500")}
        assert source == PRICE_SOURCE_OVERRIDE

    def test_build_price_oracle_dict_off_catalogue_fails_closed(self):
        """An off-catalogue market (no params) fails closed rather than guessing."""
        provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value=None):
            with pytest.raises(ValueError, match="not found"):
                provider._build_price_oracle_dict("morpho_blue", "0xnope", None, None)

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
                provider._build_price_oracle_dict("morpho_blue", "0xmarket", None, None)

    def test_build_price_oracle_dict_single_leg_no_override_uses_unit(self):
        """VIB-5775: a SINGLE-LEG (supply-only) market (loan_token=None) is valued
        from ONE collateral price — no raise. No override → unit placeholder."""
        from almanak.framework.data.position_health import PRICE_SOURCE_SAME_ASSET_UNIT

        provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value={"collateral_token": "USDC", "loan_token": None}):
            oracle, source = provider._build_price_oracle_dict("euler_v2", "usdc", None, None)
        assert oracle == {"USDC": Decimal("1")}
        assert source == PRICE_SOURCE_SAME_ASSET_UNIT

    def test_build_price_oracle_dict_single_leg_with_override_uses_price(self):
        """A single-leg market with a collateral override values from that price."""
        from almanak.framework.data.position_health import PRICE_SOURCE_OVERRIDE

        provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value={"collateral_token": "WAVAX", "loan_token": None}):
            oracle, source = provider._build_price_oracle_dict("euler_v2", "wavax", Decimal("40"), None)
        assert oracle == {"WAVAX": Decimal("40")}
        assert source == PRICE_SOURCE_OVERRIDE

    def test_build_price_oracle_dict_two_leg_partial_override_still_raises(self):
        """LOCK (VIB-5775): the line-734 partial-override raise is UNTOUCHED — a
        genuine TWO-LEG market (silo usdc/wavax, loan_token present) given exactly ONE
        override must still raise. This is why the guard supplies the loan leg rather
        than passing a partial pair; the safety raise itself is not weakened."""
        provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value={"collateral_token": "USDC", "loan_token": "WAVAX"}):
            with pytest.raises(ValueError, match="Price overrides required"):
                provider._build_price_oracle_dict("silo_v2", "usdc/wavax", Decimal("1"), None)

    def test_build_price_oracle_dict_single_leg_silo_analogue_is_general(self):
        """The single-leg branch is params-driven, not euler-specific: a silo_v2
        single-leg params shape resolves the same way (no raise)."""
        from almanak.framework.data.position_health import PRICE_SOURCE_SAME_ASSET_UNIT

        provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value={"collateral_token": "sAVAX", "loan_token": None}):
            oracle, source = provider._build_price_oracle_dict("silo_v2", "savax", None, None)
        assert oracle == {"sAVAX": Decimal("1")}
        assert source == PRICE_SOURCE_SAME_ASSET_UNIT

    def test_get_health_single_leg_supply_only_measures_zero_debt(self):
        """VIB-5775 end-to-end: a single-leg euler supply position reads MEASURED
        collateral + a measured ZERO debt (not None), threads collateral_token into
        the seam, and maps to Infinity HF (no debt) — no HealthUnavailableError."""
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        crafted = LendingAccountState(
            collateral_usd=Decimal("500"),
            debt_usd=Decimal("0"),  # MEASURED zero (supply-only), not None
            health_factor=Decimal("999999"),  # reducer's no-debt sentinel
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=None,
        )
        single_leg_params = {"collateral_token": "USDC", "loan_token": None, "comet_address": "0xVAULT"}
        with (
            patch(self._SEAM_TARGET, return_value=crafted) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=single_leg_params),
        ):
            provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
            health = provider.get_health("euler_v2", "usdc", "0xabc")

        assert health.collateral_value_usd == Decimal("500")
        assert health.debt_value_usd == Decimal("0")  # measured zero, not unmeasured
        assert health.health_factor == Decimal("Infinity")  # no debt -> Infinity
        seam_kwargs = mock_seam.call_args.kwargs
        # collateral_token threaded into the reader; single-leg oracle dict injected.
        assert seam_kwargs["collateral_token"] == "USDC"
        assert seam_kwargs["price_oracle"] == {"USDC": Decimal("1")}
        assert seam_kwargs["market_id"] == "usdc"

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


class TestProviderAdapter:
    """The built-in adapter binds PositionHealthProvider to a protocol."""

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


class TestMarketScopedHealthSeam:
    """Silo V2 / Euler V2 / BENQI: capability-driven health via the shared seam.

    VIB-4851 phase B2 (Option B): ``get_health`` dispatches on connector-declared
    capabilities — ``market_health_reader`` first (Compound V3), then the
    account-state seam with the market id passed through for every protocol
    publishing a market table. The market-scoped trio therefore gains real
    position-health support: silo/euler (non-USD-native, valuation roles
    declared) get the same price-override translation contract as Morpho;
    BENQI (USD-native qiToken reads) gets a ``None`` oracle injection. These
    tests load each connector's REAL account-state spec (pure data) and mock
    only the market catalogue + the gateway-routed read.
    """

    _SEAM_TARGET = "almanak.framework.accounting.lending_reads.read_lending_account_state"
    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"

    def _crafted_state(self, hf: str):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        return LendingAccountState(
            collateral_usd=Decimal("10"),
            debt_usd=Decimal("5"),
            health_factor=Decimal(hf),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.90"),
        )

    def test_silo_cross_asset_passes_market_id_and_prices(self):
        """Silo health passes the synthetic market id + override-keyed oracle dict."""
        cross_asset_params = {
            "collateral_token": "wstETH",
            "loan_token": "USDC",
            "comet_address": "0xsilo",
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._crafted_state("1.80")) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=cross_asset_params),
        ):
            provider = PositionHealthProvider(chain="arbitrum", gateway_client=MagicMock())
            health = provider.get_health(
                "silo_v2",
                "wsteth/usdc",
                "0xabc",
                collateral_price_usd=Decimal("2500"),
                debt_price_usd=Decimal("1"),
            )

        assert health.health_factor == Decimal("1.80")
        assert health.protocol == "silo_v2"
        seam_kwargs = mock_seam.call_args.kwargs
        assert seam_kwargs["market_id"] == "wsteth/usdc"
        assert seam_kwargs["price_oracle"] == {"wstETH": Decimal("2500"), "USDC": Decimal("1")}

    def test_silo_cross_asset_requires_prices(self):
        """Silo inherits the Morpho contract: cross-asset overrides are mandatory."""
        cross_asset_params = {"collateral_token": "wstETH", "loan_token": "USDC"}
        with patch(self._MARKET_PARAMS_TARGET, return_value=cross_asset_params):
            provider = PositionHealthProvider(chain="arbitrum", gateway_client=MagicMock())
            with pytest.raises(ValueError, match="Price overrides required"):
                provider.get_health("silo_v2", "wsteth/usdc", "0xabc")

    def test_euler_same_asset_defaults_to_one(self):
        """Euler same-asset markets need no overrides (the price cancels in HF)."""
        same_asset_params = {"collateral_token": "WETH", "loan_token": "WETH"}
        with (
            patch(self._SEAM_TARGET, return_value=self._crafted_state("1.83")) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=same_asset_params),
        ):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            health = provider.get_health("euler_v2", "weth", "0xabc")

        assert health.health_factor == Decimal("1.83")
        assert health.protocol == "euler_v2"
        assert mock_seam.call_args.kwargs["price_oracle"] == {"WETH": Decimal("1")}
        assert mock_seam.call_args.kwargs["market_id"] == "weth"

    def test_benqi_usd_native_injects_no_prices(self):
        """A no-roles params entry WITHOUT a ``collaterals`` map keeps oracle=None.

        (VIB-5911 builds the whole-account price dict ONLY from the market
        table's ``collaterals`` map; a params shape with none stays unpriced —
        fail-closed downstream, exactly the pre-fix behaviour.)
        """
        benqi_params = {
            "comet_address": "0xqiavax",
            "comptroller_address": "0xcomptroller",
        }
        with (
            patch(self._SEAM_TARGET, return_value=self._crafted_state("2.10")) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=benqi_params),
        ):
            provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
            health = provider.get_health("benqi", "savax/avax", "0xabc")

        assert health.health_factor == Decimal("2.10")
        assert health.protocol == "benqi"
        seam_kwargs = mock_seam.call_args.kwargs
        assert seam_kwargs["market_id"] == "savax/avax"
        assert seam_kwargs["price_oracle"] is None

    def test_market_scoped_protocol_requires_market_id(self):
        """A per-market protocol with no market id fails closed before reading."""
        provider = PositionHealthProvider(chain="arbitrum", gateway_client=MagicMock())
        with pytest.raises(ValueError, match="market_id is required"):
            provider.get_health("silo_v2", "", "0xabc")

    def test_market_scoped_off_catalogue_market_fails_closed(self):
        """An off-catalogue market raises 'not found' (never reads unpriced legs)."""
        with patch(self._MARKET_PARAMS_TARGET, return_value=None):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=MagicMock())
            with pytest.raises(ValueError, match="not found"):
                provider.get_health("euler_v2", "0xnope", "0xabc")

    def test_compound_still_routes_to_market_health(self):
        """Dispatch order: a market-health-capable protocol never takes the
        account-state path, even though Compound also publishes that spec."""
        provider = PositionHealthProvider(chain="ethereum", gateway_client=None)
        # The market-health path demands a gateway client up front; reaching this
        # error (rather than the account-state read) proves the routing.
        with pytest.raises(ValueError, match="GatewayClient is required to read compound_v3 health"):
            provider.get_health("compound_v3", "usdc", "0xabc")


class TestVib5911WholeAccountUsdPrices:
    """VIB-5911: a per-market protocol with NO valuation roles (BENQI) gets a
    best-effort USD-oracle dict over its ``collaterals`` map instead of ``None``.

    Root cause of the benqi TD-08/TD-15 UNVERIFIED demotion: ``price_oracle=None``
    meant ``_inject_whole_account_collateral_prices`` had nothing to inject, the
    reducer failed closed on every held asset, and the pre-reconcile read raised
    ``HealthUnavailableError`` on a loop TD-14 had already chain-verified.
    """

    _SEAM_TARGET = "almanak.framework.accounting.lending_reads.read_lending_account_state"
    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"
    _ROLES_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.valuation_roles"
    _DECLARES_TARGET = (
        "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.declares_valuation_roles"
    )

    _BENQI_PARAMS = {
        "comptroller_address": "0xcomptroller",
        "collaterals": {
            "WAVAX": {"address": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"},
            "SAVAX": {"address": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE"},
        },
    }

    @staticmethod
    def _oracle(answers: dict[str, str]):
        """Plain-callable oracle: answers listed symbols, raises for the rest."""

        def _price(symbol: str):
            if symbol in answers:
                return answers[symbol]
            raise ValueError(f"no price source for {symbol}")

        return _price

    def test_vib5911_benqi_no_roles_builds_usd_dict_from_collaterals(self):
        """Every collaterals-map symbol the oracle answers for is priced and
        injected into the seam; provenance is the USD-oracle constant."""
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
        from almanak.framework.data.position_health import PRICE_SOURCE_USD_ORACLE

        provider = PositionHealthProvider(
            chain="avalanche",
            gateway_client=MagicMock(),
            price_oracle=self._oracle({"WAVAX": "25.4", "SAVAX": "30.1"}),
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._BENQI_PARAMS):
            oracle_dict, source = provider._build_price_oracle_dict("benqi", "benqi", None, None)
        assert oracle_dict == {"WAVAX": Decimal("25.4"), "SAVAX": Decimal("30.1")}
        assert source == PRICE_SOURCE_USD_ORACLE

        # End-to-end: get_health threads the dict into the account-state seam.
        crafted = LendingAccountState(
            collateral_usd=Decimal("200"),
            debt_usd=Decimal("50"),
            health_factor=Decimal("2.4"),
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=None,
        )
        with (
            patch(self._SEAM_TARGET, return_value=crafted) as mock_seam,
            patch(self._MARKET_PARAMS_TARGET, return_value=self._BENQI_PARAMS),
        ):
            health = provider.get_health("benqi", "benqi", "0xabc")
        assert health.health_factor == Decimal("2.4")
        assert health.price_source == PRICE_SOURCE_USD_ORACLE
        assert mock_seam.call_args.kwargs["price_oracle"] == {
            "WAVAX": Decimal("25.4"),
            "SAVAX": Decimal("30.1"),
        }

    def test_vib5911_unpriced_symbol_left_out_never_unit_or_zero(self):
        """A symbol the oracle cannot answer for is ABSENT from the dict — never
        priced 1 or 0 (fatal only if held; the reducer decides, Empty ≠ Zero)."""
        from almanak.framework.data.position_health import PRICE_SOURCE_USD_ORACLE

        provider = PositionHealthProvider(
            chain="avalanche",
            gateway_client=MagicMock(),
            price_oracle=self._oracle({"WAVAX": "25.4"}),  # SAVAX unanswered
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._BENQI_PARAMS):
            oracle_dict, source = provider._build_price_oracle_dict("benqi", "benqi", None, None)
        assert oracle_dict == {"WAVAX": Decimal("25.4")}
        assert "SAVAX" not in oracle_dict
        assert source == PRICE_SOURCE_USD_ORACLE

    def test_vib5911_no_symbol_prices_returns_none_unmeasured(self):
        """No answered symbol at all (non-stables, no working oracle) keeps
        today's ``(None, "")`` — unmeasured downstream, never fabricated."""
        provider = PositionHealthProvider(
            chain="avalanche",
            gateway_client=MagicMock(),
            price_oracle=self._oracle({}),  # answers nothing
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._BENQI_PARAMS):
            oracle_dict, source = provider._build_price_oracle_dict("benqi", "benqi", None, None)
        assert oracle_dict is None
        assert source == ""

    def test_vib5911_stablecoin_table_answers_without_wired_oracle(self):
        """No wired oracle: stablecoins price from the $1 table, non-stables are
        left out — the no-oracle monitor context (unwind guard) can now measure
        a stable-only account instead of always failing."""
        from almanak.framework.data.position_health import PRICE_SOURCE_USD_ORACLE

        params = {
            "comptroller_address": "0xcomptroller",
            "collaterals": {
                "USDC": {"address": "0xusdc"},
                "WAVAX": {"address": "0xwavax"},
            },
        }
        provider = PositionHealthProvider(chain="avalanche", gateway_client=MagicMock())
        with patch(self._MARKET_PARAMS_TARGET, return_value=params):
            oracle_dict, source = provider._build_price_oracle_dict("benqi", "benqi", None, None)
        assert oracle_dict == {"USDC": Decimal("1")}
        assert "WAVAX" not in oracle_dict
        assert source == PRICE_SOURCE_USD_ORACLE

    def test_vib5911_declared_roles_resolving_empty_still_raises(self):
        """The malformed-catalogue guard is untouched: DECLARED roles that
        resolve to no symbols still fail closed (never a best-effort dict)."""
        provider = PositionHealthProvider(
            chain="avalanche",
            gateway_client=MagicMock(),
            price_oracle=self._oracle({"WAVAX": "25.4"}),
        )
        with (
            patch(self._MARKET_PARAMS_TARGET, return_value=self._BENQI_PARAMS),
            patch(self._ROLES_TARGET, return_value={}),
            patch(self._DECLARES_TARGET, return_value=True),
        ):
            with pytest.raises(ValueError, match="no collateral/loan"):
                provider._build_price_oracle_dict("benqi", "benqi", None, None)


class TestMorphoOracleDefaultPricing:
    """Cross-asset Morpho markets are priceable WITHOUT overrides.

    Every catalogued ``MORPHO_MARKETS`` entry is cross-asset (wstETH/USDC,
    WBTC/USDC, ...), so the pre-fix contract — raise ``"Price overrides
    required"`` unless the strategy injects both prices — made
    ``position_health(protocol="morpho_blue")`` unusable by default (the
    ALM-2895 "Position health unavailable" HOLD loop; VIB-5527 only gated
    pre-deploy coverage, leaving the runtime read fail-closed). The runtime
    default order is now: strategy overrides (absolute precedence) → the
    market's OWN liquidation oracle (exact) → the wired USD price oracle →
    fail closed. These tests script the gateway ``eth_call``s (oracle
    ``price()`` + Morpho ``position``/``market``) end-to-end through the REAL
    seam — only the market catalogue lookup is patched — and pin exact
    hand-computed health-factor math plus the ``price_source`` provenance.
    """

    _MARKET_PARAMS_TARGET = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params"
    _MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
    _ORACLE = "0x48F7E36EB6B826B2dF4B2E630B62Cd25e89E40e2"
    # Selectors: IOracle.price(); Morpho position(id,user) / market(id).
    _PRICE_SELECTOR = "0xa035b1fe"
    _POSITION_SELECTOR = "0x93c52062"
    _MARKET_SELECTOR = "0x5c60e39a"

    def _market_params(self) -> dict:
        # Shape-identical to the real Ethereum wstETH/USDC MORPHO_MARKETS entry.
        return {
            "collateral_token": "wstETH",
            "loan_token": "USDC",
            "oracle": self._ORACLE,
            "lltv": 860000000000000000,  # 86%
        }

    @staticmethod
    def _word(value: int) -> str:
        return format(value, "064x")

    def _scripted_gateway(
        self,
        *,
        oracle_price_raw: int | None,
        collateral_raw: int,
        borrow_shares: int,
        total_borrow_assets: int,
        total_borrow_shares: int,
    ) -> MagicMock:
        """Gateway whose ``eth_call`` routes by selector to crafted blobs.

        ``oracle_price_raw=None`` scripts an oracle fault (the ``price()`` read
        raises, which ``_gateway_eth_call`` maps to ``None`` -- a failed read).
        """
        position_blob = "0x" + self._word(0) + self._word(borrow_shares) + self._word(collateral_raw)
        market_blob = "0x" + "".join(self._word(w) for w in (0, 0, total_borrow_assets, total_borrow_shares, 0, 0))
        calls: list[tuple[str, str]] = []

        def _eth_call(chain: str, to: str, data: str, block=None) -> str:
            calls.append((to, data[:10]))
            if data.startswith(self._PRICE_SELECTOR):
                if oracle_price_raw is None:
                    raise RuntimeError("scripted oracle fault")
                assert to == self._ORACLE  # must target the catalogue oracle
                return "0x" + self._word(oracle_price_raw)
            if data.startswith(self._POSITION_SELECTOR):
                return position_blob
            if data.startswith(self._MARKET_SELECTOR):
                return market_blob
            raise AssertionError(f"unexpected selector: {data[:10]}")

        gateway = MagicMock()
        gateway.is_connected = True
        gateway.eth_call = _eth_call
        gateway.scripted_calls = calls
        return gateway

    def test_no_overrides_defaults_to_market_oracle_exact_hf(self):
        """Cross-asset market, no overrides -> numeric HF.

        Hand-computed: 2 wstETH collateral, oracle price 3000 USDC/wstETH
        (raw 3000e24, scale 1e(36+6-18)), debt 3000 USDC (shares 1:1), USDC
        loan leg -> $1 via the stablecoin table (no price oracle wired).
        HF = (2 * 3000 * 0.86) / 3000 = 1.72 exactly.
        """
        from almanak.framework.data.position_health import PRICE_SOURCE_MARKET_ORACLE

        gateway = self._scripted_gateway(
            oracle_price_raw=3000 * 10**24,
            collateral_raw=2 * 10**18,
            borrow_shares=3000 * 10**6,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=10_000 * 10**6,
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._market_params()):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=gateway)
            health = provider.get_health("morpho_blue", self._MARKET_ID, "0xabc")

        assert health.health_factor == Decimal("1.72")
        assert health.collateral_value_usd == Decimal("6000")
        assert health.debt_value_usd == Decimal("3000")
        assert health.lltv == Decimal("0.86")
        assert health.price_source == PRICE_SOURCE_MARKET_ORACLE
        assert health.to_dict()["price_source"] == PRICE_SOURCE_MARKET_ORACLE

    def test_overrides_keep_absolute_precedence_over_the_oracle(self):
        """Both overrides supplied -> the market oracle is NEVER read."""
        from almanak.framework.data.position_health import PRICE_SOURCE_OVERRIDE

        gateway = self._scripted_gateway(
            oracle_price_raw=3000 * 10**24,  # would give a different HF if consulted
            collateral_raw=2 * 10**18,
            borrow_shares=4000 * 10**6,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=10_000 * 10**6,
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._market_params()):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=gateway)
            health = provider.get_health(
                "morpho_blue",
                self._MARKET_ID,
                "0xabc",
                collateral_price_usd=Decimal("2000"),
                debt_price_usd=Decimal("1"),
            )

        # HF = (2 * 2000 * 0.86) / 4000 = 0.86 exactly -- override-priced.
        assert health.health_factor == Decimal("0.86")
        assert health.price_source == PRICE_SOURCE_OVERRIDE
        # The oracle price() read must not have happened (absolute precedence).
        assert all(sel != self._PRICE_SELECTOR for _to, sel in gateway.scripted_calls)

    def test_oracle_fault_falls_back_to_usd_oracle(self):
        """Market oracle unreadable -> the wired USD oracle prices both legs."""
        from almanak.framework.data.position_health import PRICE_SOURCE_USD_ORACLE

        gateway = self._scripted_gateway(
            oracle_price_raw=None,  # scripted oracle fault
            collateral_raw=3 * 10**18,
            borrow_shares=3000 * 10**6,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=10_000 * 10**6,
        )
        usd_quotes = {"wstETH": Decimal("2000"), "USDC": Decimal("1")}
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._market_params()):
            provider = PositionHealthProvider(
                chain="ethereum",
                gateway_client=gateway,
                price_oracle=lambda symbol: usd_quotes[symbol],
            )
            health = provider.get_health("morpho_blue", self._MARKET_ID, "0xabc")

        # HF = (3 * 2000 * 0.86) / 3000 = 1.72 exactly -- USD-oracle priced.
        assert health.health_factor == Decimal("1.72")
        assert health.price_source == PRICE_SOURCE_USD_ORACLE

    def test_fails_closed_when_no_price_source_answers(self):
        """Oracle fault + no wired USD source -> fail closed (never 1:1)."""
        gateway = self._scripted_gateway(
            oracle_price_raw=None,
            collateral_raw=2 * 10**18,
            borrow_shares=3000 * 10**6,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=10_000 * 10**6,
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._market_params()):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=gateway)
            # USDC resolves via the stablecoin table, but wstETH has no USD
            # source -> the default chain must fail closed.
            with pytest.raises(ValueError, match="Price overrides required"):
                provider.get_health("morpho_blue", self._MARKET_ID, "0xabc")

    def test_partial_override_still_fails_closed(self):
        """Exactly one override is ambiguous -> raise, never mix provenance."""
        gateway = self._scripted_gateway(
            oracle_price_raw=3000 * 10**24,
            collateral_raw=2 * 10**18,
            borrow_shares=3000 * 10**6,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=10_000 * 10**6,
        )
        with patch(self._MARKET_PARAMS_TARGET, return_value=self._market_params()):
            provider = PositionHealthProvider(chain="ethereum", gateway_client=gateway)
            with pytest.raises(ValueError, match="Price overrides required"):
                provider.get_health("morpho_blue", self._MARKET_ID, "0xabc", collateral_price_usd=Decimal("3000"))
