"""VIB-5088: chain-aware gas price defaults -- no silent flat 30 gwei.

The old ``PnLBacktestConfig.gas_price_gwei`` default was a flat 30 gwei on
every chain -- ~100x reality on Arbitrum ($1,486.72 of simulated gas on a
$10k/84-trade run). These tests pin the replacement behaviour:

- An unset ``gas_price_gwei`` resolves to the chain-aware default derived
  from the chain registry (``ChainDescriptor.gas.fallback_base_fee_gwei`` +
  ``fallback_priority_fee_gwei``); chains without registered fallbacks use
  the ethereum descriptor's values.
- The audit trail still labels the value as a default (``chain_default``
  source, ``ParameterSource.DEFAULT``) and the compliance fallback counter
  still fires -- a default is still a fabrication, just a plausible one.
- Institutional mode refuses to fabricate: an unset gas price with no
  historical/market datum raises ``NoAcceptableDataSourceError`` instead of
  silently costing trades from a guess.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.backtesting.exceptions import NoAcceptableDataSourceError
from almanak.framework.backtesting.models import ParameterSource
from almanak.framework.backtesting.pnl._engine_helpers import (
    _append_fallback_compliance_violations,
)
from almanak.framework.backtesting.pnl.config import (
    PnLBacktestConfig,
    default_gas_price_gwei_for_chain,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

# =============================================================================
# Helpers (minimal stand-ins; same shape as test_engine_characterization.py)
# =============================================================================

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 1, 2, tzinfo=UTC)


class _EmptyDataProvider:
    """Data provider that yields nothing (engine internals only)."""

    provider_name = "mock_empty"

    async def iterate(self, config: Any):  # pragma: no cover - never yields
        if False:
            yield


@dataclass
class _FakeSwapIntent:
    """Small stand-in for a SwapIntent the engine can introspect."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount: Decimal = field(default_factory=lambda: Decimal("100"))
    protocol: str = "uniswap_v3"


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=_EmptyDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _config(**overrides: Any) -> PnLBacktestConfig:
    base: dict[str, Any] = {
        "start_time": START,
        "end_time": END,
        "token_funding": _pnl_token_funding(Decimal("10000"), chain=overrides.get("chain", "arbitrum")),
    }
    base.update(overrides)
    return PnLBacktestConfig(**base)


def _market_state(gas_price_gwei: Decimal | None = None) -> MarketState:
    return MarketState(
        timestamp=START,
        prices={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        gas_price_gwei=gas_price_gwei,
    )


async def _execute_swap(
    backtester: PnLBacktester,
    config: PnLBacktestConfig,
    market_state: MarketState,
) -> Any:
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    intent = _FakeSwapIntent()
    return await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state,
        timestamp=market_state.timestamp,
        config=config,
        data_quality_tracker=None,
    )


# =============================================================================
# default_gas_price_gwei_for_chain: registry-sourced values
# =============================================================================


class TestDefaultGasPriceForChain:
    """Per-chain defaults come from the chain registry, not invented numbers."""

    @pytest.mark.parametrize(
        ("chain", "expected_gwei"),
        [
            ("arbitrum", Decimal("0.1")),  # 0.1 base + 0.0 priority
            ("base", Decimal("0.002")),  # 0.001 base + 0.001 priority
            ("optimism", Decimal("0.002")),  # 0.001 base + 0.001 priority
            ("ethereum", Decimal("0.21")),  # 0.16 base + 0.05 priority (2026-07 post-blob retune)
        ],
    )
    def test_headline_chains(self, chain: str, expected_gwei: Decimal) -> None:
        assert default_gas_price_gwei_for_chain(chain) == expected_gwei

    def test_every_registered_value_traces_to_the_descriptor(self) -> None:
        """For chains with registered fallback fees the default is exactly
        base + priority from the descriptor -- no per-module table."""
        for descriptor in ChainRegistry.all():
            gas = descriptor.gas
            if gas.fallback_base_fee_gwei is None or gas.fallback_priority_fee_gwei is None:
                continue
            expected = Decimal(str(gas.fallback_base_fee_gwei)) + Decimal(str(gas.fallback_priority_fee_gwei))
            assert default_gas_price_gwei_for_chain(descriptor.name) == expected

    def test_unknown_chain_falls_back_to_ethereum(self) -> None:
        """Documented conservative default: the ethereum descriptor values,
        mirroring the legacy DEFAULT_GAS_PRICES.get(chain, ...) shape."""
        assert default_gas_price_gwei_for_chain("not-a-chain") == default_gas_price_gwei_for_chain("ethereum")

    def test_flat_30_is_gone(self) -> None:
        for chain in ("arbitrum", "base", "optimism", "ethereum"):
            assert default_gas_price_gwei_for_chain(chain) != Decimal("30")


# =============================================================================
# PnLBacktestConfig: resolution + provenance
# =============================================================================


class TestConfigChainAwareDefault:
    def test_unset_gas_resolves_per_chain(self) -> None:
        arb = _config()  # DEFAULT_CHAIN == arbitrum
        base = _config(chain="base")
        eth = _config(chain="ethereum")

        assert arb.gas_price_gwei == Decimal("0.1")
        assert base.gas_price_gwei == Decimal("0.002")
        assert eth.gas_price_gwei == Decimal("0.21")
        assert arb.gas_price_gwei_is_default is True
        assert base.gas_price_gwei_is_default is True
        assert eth.gas_price_gwei_is_default is True

    def test_explicit_value_is_preserved_and_not_default(self) -> None:
        config = _config(gas_price_gwei=Decimal("30"))
        assert config.gas_price_gwei == Decimal("30")
        assert config.gas_price_gwei_is_default is False

    def test_explicit_value_equal_to_chain_default_is_explicit(self) -> None:
        config = _config(gas_price_gwei=Decimal("0.1"))  # == arbitrum default
        assert config.gas_price_gwei_is_default is False

    def test_negative_explicit_value_still_rejected(self) -> None:
        with pytest.raises(ValueError, match="gas_price_gwei cannot be negative"):
            _config(gas_price_gwei=Decimal("-1"))

    def test_get_gas_cost_usd_uses_resolved_default(self) -> None:
        config = _config()  # arbitrum, 0.1 gwei
        # 350_000 gas * 0.1 gwei * $3000 / 1e9 = $0.105
        assert config.get_gas_cost_usd(350_000, Decimal("3000")) == Decimal("0.105")

    def test_to_dict_records_provenance(self) -> None:
        default = _config()
        explicit = _config(gas_price_gwei=Decimal("5"))
        assert default.to_dict()["gas_price_gwei_is_default"] is True
        assert explicit.to_dict()["gas_price_gwei_is_default"] is False

    def test_round_trip_preserves_value_and_provenance(self) -> None:
        original = _config(chain="base")
        restored = PnLBacktestConfig.from_dict(original.to_dict())
        assert restored.gas_price_gwei == original.gas_price_gwei
        assert restored.gas_price_gwei_is_default is True

    def test_legacy_dict_with_flat_30_stays_explicit_30(self) -> None:
        """Pre-VIB-5088 artifacts always serialized the key; their behaviour
        must not change on reload."""
        data = _config(gas_price_gwei=Decimal("30")).to_dict()
        del data["gas_price_gwei_is_default"]  # legacy artifacts lack the flag
        restored = PnLBacktestConfig.from_dict(data)
        assert restored.gas_price_gwei == Decimal("30")
        assert restored.gas_price_gwei_is_default is False

    def test_dict_without_gas_key_resolves_chain_default(self) -> None:
        data = _config(chain="base").to_dict()
        del data["gas_price_gwei"]
        del data["gas_price_gwei_is_default"]
        restored = PnLBacktestConfig.from_dict(data)
        assert restored.gas_price_gwei == Decimal("0.002")
        assert restored.gas_price_gwei_is_default is True

    def test_hash_covers_resolved_value_not_provenance(self) -> None:
        """A default-resolved config hashes identically to an explicitly-set
        config with the same value: the simulation math depends only on the
        value."""
        defaulted = _config()
        explicit = _config(gas_price_gwei=Decimal("0.1"))
        assert defaulted.calculate_config_hash() == explicit.calculate_config_hash()

    def test_hash_differs_across_chain_defaults(self) -> None:
        assert _config().calculate_config_hash() != _config(chain="base").calculate_config_hash()


# =============================================================================
# Engine: source labels, compliance flag, audit trail
# =============================================================================


class TestEngineGasSourceProvenance:
    @pytest.mark.asyncio
    async def test_unset_gas_labels_source_chain_default(self) -> None:
        engine = _backtester()
        config = _config()

        record = await _execute_swap(engine, config, _market_state())

        assert record.gas_price_gwei == Decimal("0.1")
        assert record.metadata["gas_price_source"] == "chain_default"

    @pytest.mark.asyncio
    async def test_explicit_gas_labels_source_config(self) -> None:
        engine = _backtester()
        config = _config(gas_price_gwei=Decimal("30"))

        record = await _execute_swap(engine, config, _market_state())

        assert record.gas_price_gwei == Decimal("30")
        assert record.metadata["gas_price_source"] == "config"

    @pytest.mark.asyncio
    async def test_chain_default_still_counts_as_compliance_fallback(self) -> None:
        """The flag survives: a default is still a fabrication, just a
        plausible one."""
        engine = _backtester()
        config = _config()

        await _execute_swap(engine, config, _market_state())

        assert engine._fallback_usage is not None
        assert engine._fallback_usage["default_gas_price"] == 1

        violations: list[str] = []
        _append_fallback_compliance_violations(engine._fallback_usage, violations)
        assert any("Default gas price fallback used 1 time(s)" in v for v in violations)

    @pytest.mark.asyncio
    async def test_market_state_gas_beats_chain_default(self) -> None:
        engine = _backtester()
        config = _config()

        record = await _execute_swap(engine, config, _market_state(gas_price_gwei=Decimal("7")))

        assert record.gas_price_gwei == Decimal("7")
        assert record.metadata["gas_price_source"] == "market_state"

    def test_parameter_sources_label_default_vs_explicit(self) -> None:
        engine = _backtester()

        defaulted = engine._create_parameter_source_tracker(_config(chain="base"))
        explicit = engine._create_parameter_source_tracker(_config(gas_price_gwei=Decimal("0.1")))

        defaulted_record = next(r for r in defaulted.records if r.parameter_name == "gas_price_gwei")
        explicit_record = next(r for r in explicit.records if r.parameter_name == "gas_price_gwei")

        # Unset on a non-DEFAULT_CHAIN chain stays DEFAULT (the value-equality
        # heuristic against a DEFAULT_CHAIN-built config would get this wrong).
        assert defaulted_record.source is ParameterSource.DEFAULT
        assert defaulted_record.value == "0.002"
        # Explicitly set -- even when equal to the chain default -- is EXPLICIT.
        assert explicit_record.source is ParameterSource.EXPLICIT

    def test_parameter_sources_record_gas_eth_override(self) -> None:
        engine = _backtester()

        tracker = engine._create_parameter_source_tracker(_config(gas_eth_price_override=Decimal("3200")))

        record = next(r for r in tracker.records if r.parameter_name == "gas_eth_price_override")
        assert record.source is ParameterSource.EXPLICIT
        assert record.category == "gas"
        assert record.value == "3200"

    def test_parameter_sources_record_adapter_runtime_rates(self) -> None:
        engine = _backtester()
        engine._adapter = type("PerpLendingBacktestAdapter", (), {})()

        tracker = engine._create_parameter_source_tracker(_config(strict_reproducibility=True))

        funding = next(r for r in tracker.records if r.parameter_name == "funding_rate_source")
        apy = next(r for r in tracker.records if r.parameter_name == "apy_source")
        assert funding.source is ParameterSource.HISTORICAL
        assert apy.source is ParameterSource.HISTORICAL


# =============================================================================
# Institutional mode: refuse to fabricate
# =============================================================================


class TestInstitutionalModeGasFabrication:
    @pytest.mark.asyncio
    async def test_unset_gas_raises_instead_of_fabricating(self) -> None:
        engine = _backtester()
        config = _config(institutional_mode=True)

        with pytest.raises(NoAcceptableDataSourceError, match="refuses to fabricate"):
            await _execute_swap(engine, config, _market_state())

    @pytest.mark.asyncio
    async def test_explicit_gas_does_not_raise(self) -> None:
        engine = _backtester()
        config = _config(institutional_mode=True, gas_price_gwei=Decimal("0.1"))

        record = await _execute_swap(engine, config, _market_state())

        assert record.metadata["gas_price_source"] == "config"

    @pytest.mark.asyncio
    async def test_market_state_gas_does_not_raise(self) -> None:
        engine = _backtester()
        config = _config(institutional_mode=True)

        record = await _execute_swap(engine, config, _market_state(gas_price_gwei=Decimal("0.05")))

        assert record.metadata["gas_price_source"] == "market_state"

    @pytest.mark.asyncio
    async def test_gas_costs_disabled_does_not_raise(self) -> None:
        engine = _backtester()
        config = _config(institutional_mode=True, include_gas_costs=False)

        record = await _execute_swap(engine, config, _market_state())

        assert record.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_raise_propagates_through_intent_loop_without_handler(self) -> None:
        """_process_pending_intents must not degrade the refusal to a
        warn-and-skip (VIB-4849 fail-loud pattern)."""
        engine = _backtester()
        config = _config(institutional_mode=True)
        market_state = _market_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        with pytest.raises(NoAcceptableDataSourceError):
            await engine._process_pending_intents(
                [(_FakeSwapIntent(), START, 0)],
                portfolio,
                market_state,
                config,
            )

    @pytest.mark.asyncio
    async def test_raise_propagates_through_intent_loop_with_default_handler(self) -> None:
        """The default error handler classifies the refusal as fatal
        (should_stop) -- the backtest halts rather than skipping trades."""
        engine = _backtester()
        engine._error_handler = BacktestErrorHandler(BacktestErrorConfig())
        config = _config(institutional_mode=True)
        market_state = _market_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        with pytest.raises(NoAcceptableDataSourceError):
            await engine._process_pending_intents(
                [(_FakeSwapIntent(), START, 0)],
                portfolio,
                market_state,
                config,
            )

    @pytest.mark.asyncio
    async def test_missing_data_path_notifies_strategy_failure(self) -> None:
        """The refusal must still call ``on_intent_executed(success=False)`` so
        a strategy state machine sees the failure -- parity with the generic
        execution-error path. Without it the intent silently vanishes."""
        engine = _backtester()
        config = _config(institutional_mode=True)
        market_state = _market_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        intent = _FakeSwapIntent()
        notifications: list[tuple[Any, bool]] = []

        class _RecordingStrategy:
            def on_intent_executed(self, notified_intent: Any, success: bool, result: Any) -> None:
                notifications.append((notified_intent, success))

        with pytest.raises(NoAcceptableDataSourceError):
            await engine._process_pending_intents(
                [(intent, START, 0)],
                portfolio,
                market_state,
                config,
                strategy=_RecordingStrategy(),
            )

        assert notifications, "strategy was not notified of the missing-data failure"
        assert notifications[-1] == (intent, False)

    @pytest.mark.asyncio
    async def test_non_institutional_default_still_fabricates_with_flag(self) -> None:
        """Outside institutional mode the honest default is used and flagged,
        never raised."""
        engine = _backtester()
        config = _config(institutional_mode=False)

        record = await _execute_swap(engine, config, _market_state())

        assert record.success
        assert record.metadata["gas_price_source"] == "chain_default"
        assert engine._fallback_usage is not None
        assert engine._fallback_usage["default_gas_price"] == 1
