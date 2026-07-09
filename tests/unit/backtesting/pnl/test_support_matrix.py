"""Tests for the PnL backtest preflight support matrix (blueprint 31).

Covers ``almanak/framework/backtesting/pnl/support_matrix.py`` plus its
engine wiring:

- chain / price-lane hard failures (registry-derived, provider-vendor keyed)
- best-effort strategy-type + protocol discovery (config wins, no guessing)
- the per-lane verdicts (fee model, LP volume, lending APY, perp funding,
  intents-vs-simulated-envelope)
- institutional-mode boot compliance violations
- serialization round trips and the additive ``PreflightReport.support``
  emission rule (the ``swap:fiat_usd_pin`` discipline)
- engine integration: preflight short-circuits BEFORE any provider probe on
  a hard failure, and the abort is unconditional (not bypassable via
  ``fail_on_preflight_error=False``)

Matrix stance (blueprint 31 section 9): a preflight capability gate is not a
conservation invariant over an engine run, so it does not fit a Trust Matrix
cell; this file is the validation surface instead.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.models import PreflightReport
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.support_matrix import (
    LANE_FEE_MODEL,
    _strategy_config_dict,
    LANE_INTENTS,
    LANE_LENDING_APY,
    LANE_LP_VOLUME,
    LANE_PERP_FUNDING,
    LANE_PRICE,
    BacktestSupportReport,
    LaneSupport,
    boot_compliance_violations,
    evaluate_backtest_support,
)
from tests.backtesting_funding import pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

_START = datetime(2024, 1, 1, tzinfo=UTC)
_END = datetime(2024, 1, 1, 3, tzinfo=UTC)


def _config(chain: str = "arbitrum", **overrides: Any) -> PnLBacktestConfig:
    kwargs: dict[str, Any] = {
        "start_time": _START,
        "end_time": _END,
        "interval_seconds": 3600,
        "chain": chain,
        "tokens": ["WETH", "USDC"],
        "include_gas_costs": False,
    }
    kwargs.update(overrides)
    return PnLBacktestConfig(**kwargs)


def _lane(report: BacktestSupportReport, lane: str, protocol: str | None = None) -> LaneSupport:
    matches = [entry for entry in report.lanes if entry.lane == lane and entry.protocol == protocol]
    assert len(matches) == 1, f"expected exactly one {lane}[{protocol}] lane, got {report.lanes}"
    return matches[0]


class _VendorOnlyProvider:
    """Provider that prices via a vendor chain platform but must never be probed."""

    price_platform_vendor = "coingecko"

    @property
    def provider_name(self) -> str:
        return "vendor-only"

    async def get_price(self, token: Any, timestamp: Any) -> Decimal:
        raise AssertionError("preflight must not probe the provider after a support hard failure")

    async def iterate(self, config: Any) -> Any:
        raise AssertionError("the simulation loop must not run after a support hard failure")


class _HoldStrategy:
    """Minimal duck-typed strategy: holds forever."""

    def __init__(self, config: dict[str, Any] | None = None, deployment_id: str = "support-matrix-test") -> None:
        self.config = config or {}
        self.deployment_id = deployment_id

    def decide(self, market: Any) -> Any:
        return None


# =============================================================================
# Chain + price lane
# =============================================================================


class TestChainAndPriceLane:
    def test_unregistered_chain_is_a_hard_failure(self) -> None:
        report = evaluate_backtest_support(_config(chain="not_a_chain"))

        assert report.hard_failures == ["chain 'not_a_chain' is not a registered chain"]
        assert _lane(report, LANE_PRICE).status == "unsupported"
        assert report.has_signal
        # The remediation names registered chains from the registry, not a table.
        assert any("arbitrum" in rec for rec in report.recommendations)

    def test_vendor_platform_chain_gap_is_a_hard_failure(self) -> None:
        """solana resolves as a chain but declares no coingecko platform id."""
        report = evaluate_backtest_support(_config(chain="solana"), price_vendor="coingecko")

        assert len(report.hard_failures) == 1
        assert "solana" in report.hard_failures[0]
        assert "coingecko" in report.hard_failures[0]
        assert _lane(report, LANE_PRICE).status == "unsupported"
        assert any("arbitrum" in rec for rec in report.recommendations)

    def test_vendor_resolves_from_provider_attribute(self) -> None:
        report = evaluate_backtest_support(_config(chain="solana"), data_provider=_VendorOnlyProvider())

        assert report.hard_failures

    def test_provider_without_vendor_platform_is_not_judged(self) -> None:
        """Custom/synthetic providers keep pricing chains the vendor map omits."""
        report = evaluate_backtest_support(_config(chain="solana"), data_provider=MockDataProvider())

        assert report.hard_failures == []
        price = _lane(report, LANE_PRICE)
        assert price.status == "supported"
        assert "probed by the standard preflight" in price.detail

    def test_vendor_indexed_chain_is_supported(self) -> None:
        report = evaluate_backtest_support(_config(chain="arbitrum"), price_vendor="coingecko")

        price = _lane(report, LANE_PRICE)
        assert price.status == "supported"
        assert "arbitrum-one" in price.detail
        assert report.hard_failures == []

    def test_non_string_vendor_signal_is_ignored(self) -> None:
        """A mocked provider class (MagicMock attribute) must not fabricate a vendor."""

        class _MockedProvider:
            price_platform_vendor = object()  # e.g. MagicMock in CLI tests

        report = evaluate_backtest_support(_config(chain="solana"), data_provider=_MockedProvider())

        assert report.hard_failures == []
        assert _lane(report, LANE_PRICE).status == "supported"

    def test_chain_alias_resolves_before_the_platform_lookup(self) -> None:
        """An alias (sol -> solana) must hit the same verdict as its canonical name."""
        report = evaluate_backtest_support(_config(chain="sol"), price_vendor="coingecko")

        assert report.hard_failures
        assert report.chain == "sol"  # configured spelling preserved for the operator


# =============================================================================
# Strategy type + protocol discovery
# =============================================================================


class _FakeMetadata:
    tags = ["lp", "liquidity"]
    supported_protocols = ["uniswap_v3", "aerodrome"]
    intent_types = ["LP_OPEN", "LP_CLOSE"]


class _FakeStrategyClass:
    STRATEGY_METADATA = _FakeMetadata()


class TestStrategyConfigExtraction:
    """Direct branch coverage for the best-effort strategy.config coercion."""

    def test_class_and_none_yield_empty(self) -> None:
        assert _strategy_config_dict(None) == {}
        assert _strategy_config_dict(_FakeStrategyClass) == {}

    def test_missing_and_none_config_yield_empty(self) -> None:
        class _NoConfig:
            pass

        class _NoneConfig:
            config = None

        assert _strategy_config_dict(_NoConfig()) == {}
        assert _strategy_config_dict(_NoneConfig()) == {}

    def test_plain_dict_config_passes_through(self) -> None:
        class _DictConfig:
            config = {"protocol": "uniswap_v3"}

        assert _strategy_config_dict(_DictConfig()) == {"protocol": "uniswap_v3"}

    def test_to_dict_result_used_when_dict(self) -> None:
        class _Config:
            def to_dict(self) -> dict[str, Any]:
                return {"protocol": "aerodrome"}

        class _Strategy:
            config = _Config()

        assert _strategy_config_dict(_Strategy()) == {"protocol": "aerodrome"}

    def test_raising_to_dict_falls_through_to_dict_conversion(self) -> None:
        class _Config:
            def to_dict(self) -> dict[str, Any]:
                raise RuntimeError("boom")

            def keys(self):
                return ["protocol"]

            def __getitem__(self, key: str) -> str:
                return "curve"

        class _Strategy:
            config = _Config()

        assert _strategy_config_dict(_Strategy()) == {"protocol": "curve"}

    def test_non_dict_to_dict_falls_through(self) -> None:
        class _Config:
            def to_dict(self) -> list[str]:
                return ["not", "a", "dict"]

            def keys(self):
                return ["protocol"]

            def __getitem__(self, key: str) -> str:
                return "balancer_v2"

        class _Strategy:
            config = _Config()

        assert _strategy_config_dict(_Strategy()) == {"protocol": "balancer_v2"}

    def test_unconvertible_config_yields_empty(self) -> None:
        class _Strategy:
            config = object()

        assert _strategy_config_dict(_Strategy()) == {}


class TestDiscovery:
    def test_config_protocol_wins_over_metadata(self) -> None:
        report = evaluate_backtest_support(
            _config(),
            strategy=_FakeStrategyClass,
            strategy_config={"protocol": "Uniswap-V3"},
        )

        assert report.protocols == ["uniswap_v3"]

    def test_metadata_protocols_used_without_config_protocol(self) -> None:
        report = evaluate_backtest_support(_config(), strategy=_FakeStrategyClass, strategy_config={})

        assert report.protocols == ["uniswap_v3", "aerodrome"]
        assert report.strategy_type == "lp"  # detected from tags, class-safe

    def test_no_protocols_warns_and_skips_protocol_lanes(self) -> None:
        report = evaluate_backtest_support(_config())

        assert report.protocols == []
        assert any("protocols not declared" in warning for warning in report.warnings)
        assert [entry.lane for entry in report.lanes] == [LANE_PRICE]

    def test_pool_field_is_not_treated_as_a_protocol(self) -> None:
        report = evaluate_backtest_support(_config(), strategy_config={"pool": "WETH/USDC/500"})

        assert report.protocols == []
        assert any("protocols not declared" in warning for warning in report.warnings)

    def test_explicit_strategy_type_overrides_detection(self) -> None:
        report = evaluate_backtest_support(
            _config(),
            strategy=_FakeStrategyClass,
            strategy_config={},
            explicit_strategy_type="swap",
        )

        assert report.strategy_type == "swap"

    def test_auto_strategy_type_means_detect(self) -> None:
        report = evaluate_backtest_support(
            _config(),
            strategy=_FakeStrategyClass,
            strategy_config={},
            explicit_strategy_type="auto",
        )

        assert report.strategy_type == "lp"


# =============================================================================
# Fee model lane
# =============================================================================


class TestFeeModelLane:
    def test_registered_protocol_fee_model_is_supported(self) -> None:
        report = evaluate_backtest_support(_config(), strategy_config={"protocol": "uniswap_v3"})

        assert _lane(report, LANE_FEE_MODEL, "uniswap_v3").status == "supported"

    def test_missing_fee_model_is_degraded(self) -> None:
        report = evaluate_backtest_support(_config(), strategy_config={"protocol": "sushiswap_v3"})

        lane = _lane(report, LANE_FEE_MODEL, "sushiswap_v3")
        assert lane.status == "degraded"
        assert "flat" in lane.detail and "default fee model" in lane.detail


# =============================================================================
# LP volume lane
# =============================================================================


class TestLpVolumeLane:
    def test_disabled_historical_volume_is_degraded_even_when_declared(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "uniswap_v3"},
            data_config=BacktestDataConfig(use_historical_volume=False),
            explicit_strategy_type="lp",
        )

        lane = _lane(report, LANE_LP_VOLUME, "uniswap_v3")
        assert lane.status == "degraded"
        assert "use_historical_volume=False" in lane.detail

    def test_declared_volume_chain_is_supported(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="lp",
        )

        lane = _lane(report, LANE_LP_VOLUME, "uniswap_v3")
        assert lane.status == "supported"
        assert "gateway DEX lane" in lane.detail

    def test_undeclared_volume_chain_is_degraded_and_names_the_flags(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="lp",
        )

        lane = _lane(report, LANE_LP_VOLUME, "uniswap_v3")
        assert lane.status == "degraded"
        assert "--pool-volume-usd-daily" in lane.detail
        assert "--allow-volume-fallback" in lane.detail

    def test_explicit_volume_flag_makes_the_lane_supported(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="lp",
            data_config=BacktestDataConfig(explicit_pool_volume_usd_daily=Decimal("5000000")),
        )

        lane = _lane(report, LANE_LP_VOLUME, "uniswap_v3")
        assert lane.status == "supported"
        assert "--pool-volume-usd-daily" in lane.detail

    def test_volume_fallback_opt_in_is_degraded(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="lp",
            data_config=BacktestDataConfig(allow_volume_fallback=True),
        )

        lane = _lane(report, LANE_LP_VOLUME, "uniswap_v3")
        assert lane.status == "degraded"
        assert "LOW-confidence" in lane.detail

    def test_lane_skipped_for_non_lp_strategies(self) -> None:
        report = evaluate_backtest_support(
            _config(),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="swap",
        )

        assert not [entry for entry in report.lanes if entry.lane == LANE_LP_VOLUME]


# =============================================================================
# Lending APY lane
# =============================================================================


class TestLendingApyLane:
    def test_disabled_historical_apy_is_degraded_even_with_provider(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="ethereum"),
            strategy_config={"protocol": "aave_v3"},
            data_config=BacktestDataConfig(use_historical_apy=False),
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "aave_v3")
        assert lane.status == "degraded"
        assert "use_historical_apy=False" in lane.detail

    def test_historical_apy_provider_is_supported(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "aave_v3"},
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "aave_v3")
        assert lane.status == "supported"
        assert "aave_v3" in lane.detail

    def test_missing_provider_is_degraded_and_names_the_defaults(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "benqi"},
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "benqi")
        assert lane.status == "degraded"
        assert "APY" in lane.detail

    def test_provider_chain_gap_is_degraded(self) -> None:
        """The lane gates on the backtest provider's subgraph coverage, so a
        chain the provider does not index (monad) degrades to defaults."""
        report = evaluate_backtest_support(
            _config(chain="monad"),
            strategy_config={"protocol": "morpho_blue"},
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "morpho_blue")
        assert lane.status == "degraded"
        assert "monad" in lane.detail

    def test_live_rate_lane_chain_without_subgraph_is_degraded(self) -> None:
        """The LIVE rate lane is wider than the backtest historical subgraph.

        ``morpho_blue`` joined the arbitrum LIVE rate lane (``rate_history_chains``),
        but ``MorphoBlueAPYProvider`` still indexes only
        ethereum/base — so a BACKTEST asking for arbitrum historical APY degrades
        to defaults. The support matrix must report that honestly (P1 regression
        fix for PR #3210), not advertise it as supported off the live-lane list.
        """
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "morpho_blue"},
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "morpho_blue")
        assert lane.status == "degraded"
        assert "arbitrum" in lane.detail
        # The degrade names the provider's REAL (subgraph) coverage, not the
        # wider live rate lane.
        assert "ethereum" in lane.detail
        assert "base" in lane.detail

    def test_aave_bsc_backtest_history_is_degraded(self) -> None:
        """aave_v3 gained bsc on the LIVE rate lane, but the historical subgraph
        provider (AaveV3APYProvider) has no bsc source — backtest history degrades.
        """
        report = evaluate_backtest_support(
            _config(chain="bsc"),
            strategy_config={"protocol": "aave_v3"},
            explicit_strategy_type="lending",
        )

        lane = _lane(report, LANE_LENDING_APY, "aave_v3")
        assert lane.status == "degraded"
        assert "bsc" in lane.detail


# =============================================================================
# Perp funding lane
# =============================================================================


class TestPerpFundingLane:
    def test_disabled_historical_funding_is_degraded_even_with_provider(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "gmx_v2"},
            data_config=BacktestDataConfig(use_historical_funding=False),
            explicit_strategy_type="perp",
        )

        lane = _lane(report, LANE_PERP_FUNDING, "gmx_v2")
        assert lane.status == "degraded"
        assert "use_historical_funding=False" in lane.detail

    def test_declared_funding_chain_is_supported(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "gmx_v2"},
            explicit_strategy_type="perp",
        )

        lane = _lane(report, LANE_PERP_FUNDING, "gmx_v2")
        assert lane.status == "supported"

    def test_funding_alias_resolves(self) -> None:
        """The legacy 'gmx' spelling resolves through the registry alias."""
        report = evaluate_backtest_support(
            _config(chain="arbitrum"),
            strategy_config={"protocol": "gmx"},
            explicit_strategy_type="perp",
        )

        assert _lane(report, LANE_PERP_FUNDING, "gmx").status == "supported"

    def test_undeclared_funding_chain_is_degraded(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="base"),
            strategy_config={"protocol": "gmx_v2"},
            explicit_strategy_type="perp",
        )

        lane = _lane(report, LANE_PERP_FUNDING, "gmx_v2")
        assert lane.status == "degraded"
        assert "flat fallback" in lane.detail


# =============================================================================
# Intents lane (simulated-envelope warnings)
# =============================================================================


class TestIntentsLane:
    def test_partial_envelope_gap_warns_but_stays_supported(self) -> None:
        report = evaluate_backtest_support(_config(), strategy_config={"protocol": "uniswap_v3"})

        lane = _lane(report, LANE_INTENTS, "uniswap_v3")
        assert lane.status == "supported"
        assert "LP_COLLECT_FEES" in lane.detail
        warning = next(w for w in report.warnings if "LP_COLLECT_FEES" in w)
        assert "UnsupportedIntentError" in warning
        assert "FAILS" in warning

    def test_fully_simulatable_connector_has_no_warning(self) -> None:
        report = evaluate_backtest_support(_config(chain="base"), strategy_config={"protocol": "aerodrome"})

        lane = _lane(report, LANE_INTENTS, "aerodrome")
        assert lane.status == "supported"
        assert report.warnings == []

    def test_unknown_protocol_warns_without_guessing(self) -> None:
        report = evaluate_backtest_support(_config(), strategy_config={"protocol": "no_such_protocol"})

        assert not [entry for entry in report.lanes if entry.lane == LANE_INTENTS]
        assert any("no connector strategy manifest" in warning for warning in report.warnings)


# =============================================================================
# Institutional boot compliance violations
# =============================================================================


class TestBootComplianceViolations:
    def _degraded_report(self) -> BacktestSupportReport:
        return evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "benqi"},
            explicit_strategy_type="lending",
        )

    def test_default_mode_records_nothing(self) -> None:
        report = self._degraded_report()
        assert report.degraded_lanes  # precondition

        assert boot_compliance_violations(report, _config()) == []

    def test_institutional_mode_records_each_degraded_lane(self) -> None:
        report = self._degraded_report()

        violations = boot_compliance_violations(report, _config(institutional_mode=True))

        assert len(violations) == len(report.degraded_lanes)
        assert all(violation.startswith("Support matrix: lane") for violation in violations)

    def test_strict_reproducibility_records_too(self) -> None:
        report = self._degraded_report()

        assert boot_compliance_violations(report, _config(strict_reproducibility=True))

    def test_none_report_is_a_no_op(self) -> None:
        assert boot_compliance_violations(None, _config(institutional_mode=True)) == []


# =============================================================================
# Serialization
# =============================================================================


class TestSerialization:
    def test_support_report_round_trip(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="avalanche"),
            strategy_config={"protocol": "uniswap_v3"},
            explicit_strategy_type="lp",
        )

        restored = BacktestSupportReport.from_dict(report.to_dict())

        assert restored == report
        assert restored.to_dict() == report.to_dict()

    def test_lane_protocol_key_emitted_only_when_set(self) -> None:
        chain_lane = LaneSupport(lane=LANE_PRICE, status="supported", detail="x")
        protocol_lane = LaneSupport(lane=LANE_FEE_MODEL, status="degraded", detail="y", protocol="uniswap_v3")

        assert "protocol" not in chain_lane.to_dict()
        assert protocol_lane.to_dict()["protocol"] == "uniswap_v3"

    def test_preflight_report_emits_support_only_with_signal(self) -> None:
        """The fiat_usd_pin discipline: no new keys on all-green artifacts."""
        all_green = evaluate_backtest_support(_config(chain="base"), strategy_config={"protocol": "aerodrome"})
        assert not all_green.has_signal

        payload = PreflightReport(passed=True, support=all_green).to_dict()
        assert "support" not in payload

        degraded = evaluate_backtest_support(_config(), strategy_config={"protocol": "sushiswap_v3"})
        assert degraded.has_signal
        payload = PreflightReport(passed=True, support=degraded).to_dict()
        assert payload["support"] == degraded.to_dict()

    def test_preflight_report_from_dict_restores_support(self) -> None:
        degraded = evaluate_backtest_support(_config(), strategy_config={"protocol": "sushiswap_v3"})
        report = PreflightReport(passed=True, support=degraded)

        restored = PreflightReport.from_dict(report.to_dict())

        assert restored.support is not None
        assert restored.support.to_dict() == degraded.to_dict()

    def test_preflight_report_without_support_key_stays_none(self) -> None:
        restored = PreflightReport.from_dict({"passed": True})

        assert restored.support is None


# =============================================================================
# Engine integration
# =============================================================================


class TestEngineIntegration:
    def _backtester(self, provider: Any, **kwargs: Any) -> PnLBacktester:
        return PnLBacktester(data_provider=provider, fee_models={}, slippage_models={}, **kwargs)

    def test_hard_failure_short_circuits_before_any_provider_probe(self) -> None:
        """No data fetch after a support hard failure — the probing provider raises if touched."""
        backtester = self._backtester(_VendorOnlyProvider())
        config = _config(chain="solana")

        report = asyncio.run(backtester.run_preflight_validation(config, strategy=_HoldStrategy()))

        assert report.passed is False
        assert report.support is not None
        assert report.support.hard_failures
        assert [check.check_name for check in report.checks] == ["support_matrix"]
        assert report.checks[0].severity == "error"

    def test_backtest_aborts_even_with_fail_on_preflight_error_false(self) -> None:
        """--allow-missing-prices opts into degraded data, not an unpriceable chain."""
        backtester = self._backtester(_VendorOnlyProvider())
        config = _config(chain="solana", token_funding=pnl_token_funding(1000))
        config.fail_on_preflight_error = False

        with pytest.raises(PreflightValidationError) as exc_info:
            asyncio.run(backtester.backtest(_HoldStrategy(), config))

        assert "support" in str(exc_info.value).lower()
        assert "solana" in str(exc_info.value)

    def test_preflight_disabled_is_the_only_bypass(self) -> None:
        """preflight_validation=False skips the gate (documented escape hatch)."""
        backtester = self._backtester(MockDataProvider())
        config = _config(
            token_funding=pnl_token_funding(1000),
            preflight_validation=False,
        )

        result = asyncio.run(backtester.backtest(_HoldStrategy(), config))

        assert result.success
        assert result.preflight_report is None

    def test_degraded_lane_does_not_block_the_run(self) -> None:
        """Default mode: degraded lanes warn and continue; no boot violations."""
        backtester = self._backtester(MockDataProvider(), strategy_type="lending")
        config = _config(token_funding=pnl_token_funding(1000))
        strategy = _HoldStrategy(config={"protocol": "benqi"})

        result = asyncio.run(backtester.backtest(strategy, config))

        assert result.success
        assert result.preflight_passed is True
        assert result.preflight_report is not None
        support = result.preflight_report.support
        assert support is not None
        assert any(lane.lane == LANE_LENDING_APY and lane.status == "degraded" for lane in support.lanes)
        assert not any(v.startswith("Support matrix:") for v in result.compliance_violations)
        support_checks = [c for c in result.preflight_report.checks if c.check_name == "support_matrix"]
        assert len(support_checks) == 1
        assert support_checks[0].severity == "warning"

    def test_institutional_mode_records_boot_violations(self) -> None:
        backtester = self._backtester(MockDataProvider(), strategy_type="lending")
        config = _config(token_funding=pnl_token_funding(1000), institutional_mode=True)
        strategy = _HoldStrategy(config={"protocol": "benqi"})
        config.fail_on_preflight_error = False  # the degraded lane is warning-severity anyway

        result = asyncio.run(backtester.backtest(strategy, config))

        assert result.success
        assert any(v.startswith("Support matrix: lane 'lending_apy[benqi]'") for v in result.compliance_violations)
        assert result.institutional_compliance is False

    def test_all_clear_support_adds_no_check_row(self) -> None:
        """No support_matrix check row on an all-green run (additive artifact rule)."""
        backtester = self._backtester(MockDataProvider())
        config = _config(token_funding=pnl_token_funding(1000))

        result = asyncio.run(backtester.backtest(_HoldStrategy(), config))

        assert result.success
        assert result.preflight_report is not None
        assert not [c for c in result.preflight_report.checks if c.check_name == "support_matrix"]


# =============================================================================
# Rendering
# =============================================================================


class TestRenderTable:
    def test_table_carries_lanes_failures_and_warnings(self) -> None:
        report = evaluate_backtest_support(
            _config(chain="solana"),
            strategy_config={"protocol": "uniswap_v3"},
            price_vendor="coingecko",
        )

        table = report.render_table()

        assert "chain: solana" in table
        assert "UNSUPPORTED" in table
        assert "HARD FAILURE:" in table
        assert "uniswap_v3" in table
