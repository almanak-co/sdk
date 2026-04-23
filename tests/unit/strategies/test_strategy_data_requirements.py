"""Tests for StrategyDataRequirements + selective runner wiring (VIB-3392)."""

from __future__ import annotations

import pytest

from almanak.framework.strategies.metadata import (
    LEGACY_COMPAT_DATA_REQUIREMENTS,
    StrategyDataRequirements,
    StrategyMetadata,
    almanak_strategy,
)


# ---------------------------------------------------------------------------
# StrategyDataRequirements defaults
# ---------------------------------------------------------------------------


class TestStrategyDataRequirementsDefaults:
    def test_price_and_balance_default_true(self):
        req = StrategyDataRequirements()
        assert req.price is True
        assert req.balance is True

    def test_optional_services_default_false(self):
        req = StrategyDataRequirements()
        assert req.indicators is False
        assert req.lending_rates is False
        assert req.funding_rates is False

    def test_frozen(self):
        req = StrategyDataRequirements()
        with pytest.raises((AttributeError, TypeError)):
            req.indicators = True  # type: ignore[misc]

    def test_explicit_overrides(self):
        req = StrategyDataRequirements(
            price=True,
            balance=True,
            indicators=True,
            lending_rates=True,
            funding_rates=True,
        )
        assert req.indicators is True
        assert req.lending_rates is True
        assert req.funding_rates is True


# ---------------------------------------------------------------------------
# LEGACY_COMPAT_DATA_REQUIREMENTS sentinel
# ---------------------------------------------------------------------------


class TestLegacyCompatRequirements:
    def test_all_services_enabled(self):
        req = LEGACY_COMPAT_DATA_REQUIREMENTS
        assert req.price is True
        assert req.balance is True
        assert req.indicators is True
        assert req.lending_rates is True
        assert req.funding_rates is True


# ---------------------------------------------------------------------------
# StrategyMetadata default field
# ---------------------------------------------------------------------------


class TestStrategyMetadataDataRequirements:
    def test_default_is_minimal(self):
        meta = StrategyMetadata(name="test")
        assert meta.data_requirements.indicators is False
        assert meta.data_requirements.lending_rates is False
        assert meta.data_requirements.funding_rates is False

    def test_to_dict_includes_data_requirements(self):
        req = StrategyDataRequirements(indicators=True)
        meta = StrategyMetadata(name="test", data_requirements=req)
        d = meta.to_dict()
        assert "data_requirements" in d
        assert d["data_requirements"]["indicators"] is True
        assert d["data_requirements"]["lending_rates"] is False

    def test_to_dict_all_fields_present(self):
        meta = StrategyMetadata(name="test")
        d = meta.to_dict()
        dr = d["data_requirements"]
        assert set(dr.keys()) == {"price", "balance", "indicators", "lending_rates", "funding_rates"}


# ---------------------------------------------------------------------------
# @almanak_strategy decorator — data_requirements normalization
# ---------------------------------------------------------------------------


class TestDecoratorDataRequirements:
    def test_omitted_uses_legacy_compat(self):
        @almanak_strategy(name="test_omit", supported_chains=["arbitrum"])
        class S:
            pass

        assert S.STRATEGY_METADATA.data_requirements == LEGACY_COMPAT_DATA_REQUIREMENTS

    def test_explicit_instance_used(self):
        req = StrategyDataRequirements(indicators=True)

        @almanak_strategy(
            name="test_explicit",
            supported_chains=["arbitrum"],
            data_requirements=req,
        )
        class S:
            pass

        assert S.STRATEGY_METADATA.data_requirements.indicators is True
        assert S.STRATEGY_METADATA.data_requirements.lending_rates is False

    def test_dict_normalized_to_dataclass(self):
        @almanak_strategy(
            name="test_dict",
            supported_chains=["arbitrum"],
            data_requirements={"indicators": True, "lending_rates": True},
        )
        class S:
            pass

        req = S.STRATEGY_METADATA.data_requirements
        assert isinstance(req, StrategyDataRequirements)
        assert req.indicators is True
        assert req.lending_rates is True
        assert req.funding_rates is False

    def test_minimal_requirements(self):
        @almanak_strategy(
            name="test_minimal",
            supported_chains=["arbitrum"],
            data_requirements=StrategyDataRequirements(),
        )
        class S:
            pass

        req = S.STRATEGY_METADATA.data_requirements
        assert req.indicators is False
        assert req.lending_rates is False
        assert req.funding_rates is False

    def test_dict_with_unknown_field_raises(self):
        with pytest.raises(TypeError):
            @almanak_strategy(
                name="test_bad_dict",
                supported_chains=["arbitrum"],
                data_requirements={"not_a_field": True},
            )
            class S:
                pass


# ---------------------------------------------------------------------------
# _get_data_requirements helper
# ---------------------------------------------------------------------------


class TestGetDataRequirements:
    def test_decorated_strategy_returns_requirements(self):
        from almanak.framework.cli.run_helpers import _get_data_requirements

        @almanak_strategy(
            name="test_get_req",
            supported_chains=["arbitrum"],
            data_requirements=StrategyDataRequirements(indicators=True),
        )
        class S:
            pass

        req = _get_data_requirements(S())
        assert req.indicators is True

    def test_no_metadata_returns_legacy_compat(self):
        from almanak.framework.cli.run_helpers import _get_data_requirements

        class Bare:
            pass

        req = _get_data_requirements(Bare())
        assert req == LEGACY_COMPAT_DATA_REQUIREMENTS

    def test_legacy_omitted_returns_legacy_compat(self):
        from almanak.framework.cli.run_helpers import _get_data_requirements

        @almanak_strategy(name="test_legacy_compat", supported_chains=["arbitrum"])
        class S:
            pass

        req = _get_data_requirements(S())
        assert req == LEGACY_COMPAT_DATA_REQUIREMENTS


# ---------------------------------------------------------------------------
# _wire_core_providers — wiring correctness without indicator calculators
# ---------------------------------------------------------------------------


class TestWireCoreProviders:
    """Verify that _wire_core_providers correctly sets _price_oracle and _balance_provider.

    These tests catch the P1 regression where strategies with indicators=False
    ended up with None providers because wiring was inside _wire_indicators.
    """

    def _make_mock_price_oracle(self):
        from decimal import Decimal
        from unittest.mock import AsyncMock

        oracle = AsyncMock()
        oracle.get_aggregated_price = AsyncMock(return_value=Decimal("100"))
        return oracle

    def _make_mock_balance_provider(self):
        from unittest.mock import AsyncMock

        provider = AsyncMock()
        provider.get_balance = AsyncMock(return_value=None)
        return provider

    def test_wires_price_oracle_onto_strategy(self):
        from almanak.framework.cli.run import _wire_core_providers

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None

        instance = StrategyStub()
        price_oracle = self._make_mock_price_oracle()
        balance_provider = self._make_mock_balance_provider()

        _wire_core_providers(instance, price_oracle, balance_provider)

        assert instance._price_oracle is not None
        assert callable(instance._price_oracle)

    def test_wires_balance_provider_onto_strategy(self):
        from almanak.framework.cli.run import _wire_core_providers

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None

        instance = StrategyStub()
        _wire_core_providers(instance, self._make_mock_price_oracle(), self._make_mock_balance_provider())

        assert instance._balance_provider is not None
        assert callable(instance._balance_provider)

    def test_no_indicator_provider_wired(self):
        """_wire_core_providers must NOT set _indicator_provider (indicators=False path)."""
        from almanak.framework.cli.run import _wire_core_providers

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None
            _indicator_provider = None

        instance = StrategyStub()
        _wire_core_providers(instance, self._make_mock_price_oracle(), self._make_mock_balance_provider())

        assert instance._indicator_provider is None

    def test_no_rsi_provider_wired(self):
        """_wire_core_providers must NOT set _rsi_provider (indicators=False path)."""
        from almanak.framework.cli.run import _wire_core_providers

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None
            _rsi_provider = None

        instance = StrategyStub()
        _wire_core_providers(instance, self._make_mock_price_oracle(), self._make_mock_balance_provider())

        assert instance._rsi_provider is None

    def test_skips_instance_without_price_oracle_attr(self):
        """Strategy stubs lacking _price_oracle are skipped gracefully (no AttributeError)."""
        from almanak.framework.cli.run import _wire_core_providers

        class BareStub:
            pass

        instance = BareStub()
        _wire_core_providers(instance, self._make_mock_price_oracle(), self._make_mock_balance_provider())
        assert not hasattr(instance, "_price_oracle")


# ---------------------------------------------------------------------------
# Wiring gate: indicators=False still wires price/balance (P1 regression guard)
# ---------------------------------------------------------------------------


class TestWiringGateRequirementsPrice:
    """Verify that requirements.price/balance control wiring, not requirements.indicators."""

    def test_indicators_false_still_has_wired_price_oracle(self):
        """The P1 regression: indicators=False must NOT leave _price_oracle as None."""
        from unittest.mock import AsyncMock, MagicMock, patch
        from decimal import Decimal

        price_oracle = AsyncMock()
        price_oracle.get_aggregated_price = AsyncMock(return_value=Decimal("100"))
        balance_provider = AsyncMock()

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None

        instance = StrategyStub()
        req = StrategyDataRequirements(indicators=False, price=True, balance=True)

        # Simulate the wiring gate logic from _build_orchestrator_and_providers
        from almanak.framework.cli.run import _wire_core_providers, _wire_indicators

        if req.indicators:
            # Would call _wire_indicators — not called here
            pass
        elif req.price or req.balance:
            _wire_core_providers(instance, price_oracle, balance_provider)

        assert instance._price_oracle is not None, (
            "strategy._price_oracle must be wired even when indicators=False"
        )

    def test_indicators_true_wires_via_wire_indicators(self):
        """When indicators=True, _wire_indicators wires price, balance, rsi, and indicator suite."""
        from decimal import Decimal
        from unittest.mock import AsyncMock, MagicMock

        from almanak.framework.cli.run import _wire_indicators

        price_oracle = AsyncMock()
        price_oracle.get_aggregated_price = AsyncMock(return_value=Decimal("100"))
        balance_provider = AsyncMock()
        # RoutingOHLCVProvider is only used to construct calculators; a MagicMock is sufficient
        ohlcv_provider = MagicMock()

        class StrategyStub:
            _price_oracle = None
            _balance_provider = None
            _rsi_provider = None
            _indicator_provider = None

        instance = StrategyStub()
        _wire_indicators(instance, ohlcv_provider, price_oracle, balance_provider)

        assert instance._price_oracle is not None, "_wire_indicators must set _price_oracle"
        assert instance._balance_provider is not None, "_wire_indicators must set _balance_provider"
        assert instance._rsi_provider is not None, "_wire_indicators must set _rsi_provider"
        assert instance._indicator_provider is not None, "_wire_indicators must set _indicator_provider"
