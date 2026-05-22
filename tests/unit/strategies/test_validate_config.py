"""Tests for the ``IntentStrategy.validate_config()`` lifecycle hook.

The hook allows strategies to enforce preconditions on their configuration.
It is called from :py:meth:`IntentStrategy.__init__` AFTER the config is loaded
(via ``super().__init__``) and BEFORE any other setup that depends on config.

These tests cover:
    1. Default (base-class) implementation is a no-op and does not break
       existing strategies.
    2. A subclass that overrides ``validate_config()`` has it invoked during
       ``__init__``.
    3. ``ConfigValidationError.field`` is stored as an attribute and rendered
       into ``str(error)``.
    4. The hook fires BEFORE chain/wallet wiring completes, so a failure
       prevents instantiation cleanly.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.strategies import (
    ConfigValidationError,
    IntentStrategy,
)


# ---------------------------------------------------------------------------
# Concrete IntentStrategy subclasses used by these tests. They stub the
# required abstract methods (decide / get_open_positions / generate_teardown_intents)
# so we can instantiate them directly.
# ---------------------------------------------------------------------------


class _NoopStrategy(IntentStrategy):
    """Strategy that does not override ``validate_config()``."""

    STRATEGY_NAME = "noop_test_strategy"

    def decide(self, market):
        return None

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.deployment_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        return []


class _OverrideCounterStrategy(_NoopStrategy):
    """Strategy whose ``validate_config()`` is a no-op but records invocations."""

    STRATEGY_NAME = "override_counter"

    def __init__(self, *args, **kwargs):
        # IMPORTANT: set counter BEFORE super().__init__ because validate_config
        # is invoked from super().__init__.
        self._validate_calls = 0
        super().__init__(*args, **kwargs)

    def validate_config(self) -> None:
        self._validate_calls += 1


class _RaisingStrategy(_NoopStrategy):
    """Strategy whose ``validate_config()`` unconditionally fails."""

    STRATEGY_NAME = "raising_strategy"

    def validate_config(self) -> None:
        size = self.get_config("trade_size_usd", 0)
        if Decimal(str(size)) <= 0:
            raise ConfigValidationError(
                "trade_size_usd must be > 0",
                field="trade_size_usd",
            )


class _CrossFieldStrategy(_NoopStrategy):
    """Strategy whose ``validate_config()`` enforces a cross-field invariant."""

    STRATEGY_NAME = "cross_field_strategy"

    def validate_config(self) -> None:
        oversold = Decimal(str(self.get_config("rsi_oversold", 30)))
        overbought = Decimal(str(self.get_config("rsi_overbought", 70)))
        if oversold >= overbought:
            # No field attribution for cross-field errors.
            raise ConfigValidationError(
                "rsi_oversold must be strictly less than rsi_overbought",
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make(cls, config: dict | None = None) -> IntentStrategy:
    """Instantiate an IntentStrategy subclass with minimal kwargs."""
    return cls(
        config=config if config is not None else {"deployment_id": cls.STRATEGY_NAME},
        chain="arbitrum",
        wallet_address="0x000000000000000000000000000000000000dEaD",
    )


# ---------------------------------------------------------------------------
# ConfigValidationError shape
# ---------------------------------------------------------------------------


class TestConfigValidationError:
    """``ConfigValidationError`` exposes ``message`` and ``field`` attributes."""

    def test_with_field(self):
        err = ConfigValidationError("trade_size_usd must be > 0", field="trade_size_usd")
        assert err.message == "trade_size_usd must be > 0"
        assert err.field == "trade_size_usd"
        assert str(err) == "Config validation failed for 'trade_size_usd': trade_size_usd must be > 0"

    def test_without_field(self):
        err = ConfigValidationError("bad config overall")
        assert err.message == "bad config overall"
        assert err.field is None
        assert str(err) == "Config validation failed: bad config overall"

    def test_field_default_is_none(self):
        """``field`` defaults to ``None`` when omitted."""
        err = ConfigValidationError("boom")
        assert err.field is None

    def test_is_exception_subclass(self):
        """Must subclass Exception so it can be raised/caught normally."""
        assert issubclass(ConfigValidationError, Exception)

    def test_repr_contains_both_attrs(self):
        err = ConfigValidationError("msg", field="f")
        r = repr(err)
        assert "ConfigValidationError" in r
        assert "msg" in r
        assert "f" in r


# ---------------------------------------------------------------------------
# IntentStrategy.validate_config() default no-op
# ---------------------------------------------------------------------------


class TestDefaultValidateConfigIsNoop:
    """The base-class ``validate_config()`` is a no-op so legacy strategies work."""

    def test_default_strategy_instantiates(self):
        strategy = _make(_NoopStrategy)
        assert strategy.chain == "arbitrum"
        assert strategy.wallet_address.endswith("dEaD")

    def test_default_validate_config_returns_none(self):
        strategy = _make(_NoopStrategy)
        assert strategy.validate_config() is None

    def test_default_validate_config_can_be_called_repeatedly(self):
        """Safe to call multiple times (no side effects)."""
        strategy = _make(_NoopStrategy)
        for _ in range(3):
            assert strategy.validate_config() is None


# ---------------------------------------------------------------------------
# Override invocation during __init__
# ---------------------------------------------------------------------------


class TestOverrideInvokedDuringInit:
    """A subclass override of ``validate_config()`` is invoked from ``__init__``."""

    def test_override_called_exactly_once(self):
        strategy = _make(_OverrideCounterStrategy)
        assert strategy._validate_calls == 1

    def test_override_raises_propagates_from_init(self):
        """Failures surface as ConfigValidationError from ``__init__``."""
        with pytest.raises(ConfigValidationError) as excinfo:
            _make(
                _RaisingStrategy,
                config={"deployment_id": "raise_test", "trade_size_usd": 0},
            )
        assert excinfo.value.field == "trade_size_usd"
        assert "trade_size_usd" in excinfo.value.message

    def test_override_passes_with_valid_config(self):
        strategy = _make(
            _RaisingStrategy,
            config={"deployment_id": "raise_test", "trade_size_usd": Decimal("100")},
        )
        # Instantiation succeeded -- validate_config didn't raise.
        assert strategy.chain == "arbitrum"

    def test_cross_field_validation_without_field_attr(self):
        with pytest.raises(ConfigValidationError) as excinfo:
            _make(
                _CrossFieldStrategy,
                config={
                    "deployment_id": "cross",
                    "rsi_oversold": 70,
                    "rsi_overbought": 30,
                },
            )
        assert excinfo.value.field is None
        assert "rsi_oversold" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Ordering: validate_config runs BEFORE chain/wallet wiring
# ---------------------------------------------------------------------------


class TestValidationRunsBeforeSetup:
    """If ``validate_config()`` raises, no chain/wallet setup side effects leak.

    Instantiation failure should leave no partially-constructed instance
    bound to the caller (Python's standard behaviour). What matters is that
    the framework calls ``validate_config()`` BEFORE wiring up chain/wallet so
    that a preflight check catches bad configs early.
    """

    def test_failure_prevents_instantiation(self):
        """A raising validate_config prevents the instance from being returned."""
        local: dict = {}
        with pytest.raises(ConfigValidationError):
            local["s"] = _make(
                _RaisingStrategy,
                config={"deployment_id": "raise_test", "trade_size_usd": 0},
            )
        # "s" never got assigned because __init__ raised.
        assert "s" not in local

    def test_validate_config_can_access_loaded_config(self):
        """``validate_config()`` runs AFTER config load, so ``self.config`` is usable."""

        captured: dict = {}

        class _CaptureStrategy(_NoopStrategy):
            STRATEGY_NAME = "capture_config"

            def validate_config(self) -> None:
                captured["config_seen"] = self.config
                captured["trade_size"] = self.get_config("trade_size_usd", None)

        _make(
            _CaptureStrategy,
            config={"deployment_id": "capture_config", "trade_size_usd": "42"},
        )
        assert captured["config_seen"] == {
            "deployment_id": "capture_config",
            "trade_size_usd": "42",
        }
        assert captured["trade_size"] == "42"


# ---------------------------------------------------------------------------
# Import paths: ConfigValidationError is re-exported at the expected places.
# ---------------------------------------------------------------------------


class TestConfigValidationErrorImportPaths:
    """Covers the re-export chain so the Portfolio Manager can import cleanly."""

    def test_import_from_strategies_package(self):
        from almanak.framework.strategies import ConfigValidationError as A

        assert A is ConfigValidationError

    def test_import_from_strategies_exceptions_module(self):
        from almanak.framework.strategies.exceptions import ConfigValidationError as B

        assert B is ConfigValidationError

    def test_import_from_framework(self):
        from almanak.framework import ConfigValidationError as C

        assert C is ConfigValidationError

    def test_import_from_top_level_almanak(self):
        from almanak import ConfigValidationError as D

        assert D is ConfigValidationError


# ---------------------------------------------------------------------------
# Chain context is wired BEFORE validate_config() runs (audit fix P2).
# ---------------------------------------------------------------------------


class TestChainContextAvailableInValidateConfig:
    """``self.chain`` / ``self.chains`` must reflect the constructor-passed
    chain while ``validate_config()`` runs, so subclasses can validate
    chain-dependent invariants (supported pairs, per-chain limits, etc.).
    """

    def test_self_chain_reflects_constructor_chain(self):
        captured: dict = {}

        class _ChainAwareStrategy(_NoopStrategy):
            STRATEGY_NAME = "chain_aware"

            def validate_config(self) -> None:
                captured["chain"] = self.chain
                captured["chains"] = list(self.chains)
                captured["wallet"] = self.wallet_address

        _make(
            _ChainAwareStrategy,
            config={"deployment_id": "chain_aware"},
        )
        # Constructor passed chain="arbitrum" -- hook must see it, not the
        # StrategyBase default of "unknown".
        assert captured["chain"] == "arbitrum"
        assert captured["chains"] == ["arbitrum"]
        assert captured["wallet"].endswith("dEaD")

    def test_chain_dependent_validation_can_reject(self):
        """A chain-gated validator rejects the wrong chain at construction."""

        class _OnlyBaseStrategy(_NoopStrategy):
            STRATEGY_NAME = "only_base"

            def validate_config(self) -> None:
                if self.chain != "base":
                    raise ConfigValidationError(
                        f"only supported on base, got {self.chain}",
                        field="chain",
                    )

        # arbitrum -> rejected
        with pytest.raises(ConfigValidationError) as excinfo:
            _make(_OnlyBaseStrategy, config={"deployment_id": "only_base"})
        assert excinfo.value.field == "chain"
        assert "arbitrum" in excinfo.value.message

        # base -> accepted
        s = _OnlyBaseStrategy(
            config={"deployment_id": "only_base"},
            chain="base",
            wallet_address="0x000000000000000000000000000000000000dEaD",
        )
        assert s.chain == "base"


# ---------------------------------------------------------------------------
# Hot-reload re-validation and rollback (audit fix P1).
# ---------------------------------------------------------------------------


class TestHotReloadValidateConfig:
    """``update_config()`` must re-run ``validate_config()`` after applying
    the mutation and roll back on failure so invariants enforced at startup
    cannot be bypassed at runtime.
    """

    @staticmethod
    def _hot_reload_strategy(
        trade_size_usd: Decimal = Decimal("100"),
        max_slippage: Decimal = Decimal("0.005"),
    ):
        """Build a strategy whose validate_config() enforces a runtime
        invariant on HotReloadableConfig fields.
        """
        from almanak.framework.models.hot_reload_config import HotReloadableConfig

        class _TradeSizeMinStrategy(_NoopStrategy):
            STRATEGY_NAME = "trade_size_min"

            # Enforce trade_size_usd >= 50 -- catchable both at startup AND
            # when someone tries to hot-reload trade_size_usd below 50.
            def validate_config(self) -> None:
                size = Decimal(str(self.get_config("trade_size_usd", "0")))
                if size < Decimal("50"):
                    raise ConfigValidationError(
                        f"trade_size_usd must be >= 50, got {size}",
                        field="trade_size_usd",
                    )

        cfg = HotReloadableConfig(
            trade_size_usd=trade_size_usd,
            max_slippage=max_slippage,
        )
        return _TradeSizeMinStrategy(
            config=cfg,
            chain="arbitrum",
            wallet_address="0x000000000000000000000000000000000000dEaD",
        )

    def test_hot_reload_rejects_invalid_update_and_rolls_back(self):
        """Hot-reload that would violate validate_config must be rejected
        AND the previous config value must be restored."""
        strategy = self._hot_reload_strategy(trade_size_usd=Decimal("100"))
        assert strategy.config.trade_size_usd == Decimal("100")

        # Try to push trade_size_usd below the 50 floor -- must be rejected.
        result = strategy.update_config({"trade_size_usd": Decimal("20")})

        assert result.success is False
        assert result.error is not None
        assert "validate_config" in result.error
        assert "trade_size_usd" in result.error

        # Critical: the live config must still hold the OLD value.
        assert strategy.config.trade_size_usd == Decimal("100"), (
            "validate_config rejection must roll back the config mutation; "
            "found stale invalid value left in place"
        )

    def test_hot_reload_accepts_valid_update(self):
        """A valid update still applies normally."""
        strategy = self._hot_reload_strategy(trade_size_usd=Decimal("100"))
        result = strategy.update_config({"trade_size_usd": Decimal("200")})
        assert result.success is True
        assert strategy.config.trade_size_usd == Decimal("200")

    def test_hot_reload_unrelated_field_passes(self):
        """Updating a field that validate_config() does not check must pass."""
        strategy = self._hot_reload_strategy(max_slippage=Decimal("0.005"))
        result = strategy.update_config({"max_slippage": Decimal("0.01")})
        assert result.success is True
        assert strategy.config.max_slippage == Decimal("0.01")

    def test_strategy_base_without_validate_config_still_works(self):
        """update_config must remain compatible with StrategyBase subclasses
        that never define a validate_config() hook (duck-typed fallback).
        """
        # _NoopStrategy inherits the base no-op validate_config, so updates
        # should work exactly as before the hook was added.
        from almanak.framework.models.hot_reload_config import HotReloadableConfig

        cfg = HotReloadableConfig(trade_size_usd=Decimal("100"))
        strategy = _NoopStrategy(
            config=cfg,
            chain="arbitrum",
            wallet_address="0x000000000000000000000000000000000000dEaD",
        )
        result = strategy.update_config({"trade_size_usd": Decimal("200")})
        assert result.success is True
        assert strategy.config.trade_size_usd == Decimal("200")
