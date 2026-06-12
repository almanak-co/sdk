"""Unit tests for the BacktestRiskRegistry and BacktestRiskDecl (plan 022).

These tests pin:
  (a) the registry is populated with the expected six connectors;
  (b) a connector without backtest_risk contributes nothing;
  (c) alias lookups (``"morpho"`` / ``"gmx"``) resolve to the declared rows
      via ``LiquidationParamRegistry``;
  (d) ``BacktestRiskDecl.__post_init__`` validation rejects bad inputs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest


class TestBacktestRiskRegistryPopulation:
    """The registry is populated from the connector manifests (plan 022)."""

    def test_all_six_connectors_registered(self) -> None:
        """Exactly the six connectors declared in plan 022 must be registered."""
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        registered = set(BACKTEST_RISK_REGISTRY.liquidation_params())
        expected = {"aave_v3", "compound_v3", "morpho_blue", "spark", "gmx_v2", "hyperliquid"}
        assert registered == expected

    def test_connector_without_backtest_risk_absent(self) -> None:
        """Connectors that declare no backtest_risk must not appear in the registry."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        registered = set(BACKTEST_RISK_REGISTRY.liquidation_params())
        for c in CONNECTOR_REGISTRY.all():
            if c.backtest_risk is None:
                assert c.name not in registered, (
                    f"{c.name!r} has no backtest_risk but appears in BACKTEST_RISK_REGISTRY"
                )

    def test_every_registered_connector_has_decl(self) -> None:
        """Every registry entry must come from a connector with a backtest_risk decl."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        connectors_with_risk = {c.name for c in CONNECTOR_REGISTRY.all() if c.backtest_risk is not None}
        for name in BACKTEST_RISK_REGISTRY.liquidation_params():
            assert name in connectors_with_risk, (
                f"{name!r} is registered but its connector declares no backtest_risk"
            )


class TestAliasLookupViaLiquidationRegistry:
    """Alias lookups via ``LiquidationParamRegistry`` resolve to the correct rows."""

    def test_morpho_alias_resolves(self) -> None:
        """``get_params("morpho")`` must return the morpho_blue connector's default."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        registry = LiquidationParamRegistry()
        params = registry.get_params("morpho")
        assert params.liquidation_threshold == Decimal("0.825")
        assert params.maintenance_margin == Decimal("0")
        assert params.liquidation_penalty == Decimal("0.05")
        assert params.protocol == "morpho"
        assert params.source == LiquidationParamSource.PROTOCOL_DEFAULT

    def test_gmx_alias_resolves(self) -> None:
        """``get_params("gmx")`` must return the gmx_v2 connector's default."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        registry = LiquidationParamRegistry()
        params = registry.get_params("gmx")
        assert params.liquidation_threshold == Decimal("0")
        assert params.maintenance_margin == Decimal("0.01")
        assert params.liquidation_penalty == Decimal("0.05")
        assert params.protocol == "gmx"
        assert params.source == LiquidationParamSource.PROTOCOL_DEFAULT

    def test_gmx_v2_also_resolves(self) -> None:
        """``get_params("gmx_v2")`` must also resolve (gmx_v2 registers both keys)."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        registry = LiquidationParamRegistry()
        params = registry.get_params("gmx_v2")
        assert params.maintenance_margin == Decimal("0.01")
        assert params.protocol == "gmx_v2"
        assert params.source == LiquidationParamSource.PROTOCOL_DEFAULT

    def test_gmx_asset_params_key(self) -> None:
        """gmx_v2 asset rows use the ``"gmx_v2"`` key (not ``"gmx"``)."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        registry = LiquidationParamRegistry()
        params = registry.get_params("gmx_v2", "ETH")
        assert params.maintenance_margin == Decimal("0.01")
        assert params.source == LiquidationParamSource.ASSET_SPECIFIC

    def test_off_platform_venues_present(self) -> None:
        """binance_perp, bybit, dydx must resolve from OFF_PLATFORM_VENUE_DEFAULTS."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
        )

        registry = LiquidationParamRegistry()
        for venue, expected_margin in [
            ("binance_perp", Decimal("0.04")),
            ("bybit", Decimal("0.05")),
            ("dydx", Decimal("0.03")),
        ]:
            params = registry.get_params(venue)
            assert params.maintenance_margin == expected_margin, (
                f"{venue}: margin {params.maintenance_margin!r} != {expected_margin!r}"
            )


class TestBacktestRiskDeclValidation:
    """BacktestRiskDecl.__post_init__ rejects invalid inputs."""

    def test_invalid_liquidation_default_type(self) -> None:
        """liquidation_default must be None or a LiquidationDefault."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="LiquidationDefault"):
            BacktestRiskDecl(liquidation_default="not_a_decl")  # type: ignore[arg-type]

    def test_invalid_asset_params_type(self) -> None:
        """liquidation_asset_params must be None or a Mapping."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="Mapping"):
            BacktestRiskDecl(liquidation_asset_params="not_a_mapping")  # type: ignore[arg-type]

    def test_asset_key_must_be_uppercase(self) -> None:
        """Asset keys in liquidation_asset_params must be UPPER-cased."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="UPPER-cased"):
            BacktestRiskDecl(
                liquidation_asset_params={
                    "eth": (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
                }
            )

    def test_asset_value_must_be_3_tuple(self) -> None:
        """Asset values must be 3-tuples (threshold, margin, penalty)."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="3-tuple"):
            BacktestRiskDecl(
                liquidation_asset_params={
                    "ETH": (Decimal("0.86"), Decimal("0.05")),  # 2-tuple, not 3
                }
            )

    def test_valid_decl_constructs(self) -> None:
        """A valid BacktestRiskDecl constructs without error."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

        decl = BacktestRiskDecl(
            liquidation_default=LiquidationDefault(
                liquidation_threshold=Decimal("0.80"),
                maintenance_margin=Decimal("0"),
                liquidation_penalty=Decimal("0.05"),
            ),
            liquidation_asset_params={
                "ETH": (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
            },
        )
        assert decl.liquidation_default is not None
        assert "ETH" in (decl.liquidation_asset_params or {})

    def test_legacy_param_keys_rejects_uppercase(self) -> None:
        """legacy_param_keys must be lowercase strings."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="lowercase"):
            BacktestRiskDecl(legacy_param_keys=("Morpho",))

    def test_legacy_param_keys_rejects_duplicates(self) -> None:
        """legacy_param_keys must not contain duplicate strings."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="duplicates"):
            BacktestRiskDecl(legacy_param_keys=("gmx", "gmx"))

    def test_legacy_param_keys_rejects_empty_string(self) -> None:
        """legacy_param_keys must not contain empty strings."""
        from almanak.connectors._connector_descriptor import BacktestRiskDecl

        with pytest.raises(ValueError, match="non-empty"):
            BacktestRiskDecl(legacy_param_keys=("",))

    def test_morpho_blue_legacy_key_resolves(self) -> None:
        """morpho_blue connector declares legacy_param_keys=('morpho',)."""
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        decl = BACKTEST_RISK_REGISTRY.get("morpho_blue")
        assert decl is not None
        assert decl.legacy_param_keys == ("morpho",)

    def test_default_empty_legacy_keys_resolves_to_connector_name(self) -> None:
        """Connectors without legacy_param_keys fall back to connector name in LiquidationParamRegistry."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        registry = LiquidationParamRegistry()
        # aave_v3 has no legacy_param_keys; its protocol_defaults key == connector name.
        params = registry.get_params("aave_v3")
        assert params.protocol == "aave_v3"
        assert params.source == LiquidationParamSource.PROTOCOL_DEFAULT
