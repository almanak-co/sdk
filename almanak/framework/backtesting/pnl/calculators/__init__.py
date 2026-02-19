"""Calculator modules for PnL backtesting.

This package provides specialized calculators for complex financial calculations
needed during backtesting, including:

- Impermanent Loss: Calculate IL for Uniswap V3 concentrated liquidity positions
- Funding Rates: Calculate funding payments for perpetual positions
- Liquidation: Calculate liquidation prices for perpetual positions
- Interest Accrual: Calculate interest for lending positions
- Health Factor: Calculate health factors for lending positions
- Benchmark Metrics: Calculate IR, Beta, Alpha for benchmark comparison
- PnL Attribution: Attribute PnL by protocol, intent type, and asset

Example:
    from almanak.framework.backtesting.pnl.calculators import (
        ImpermanentLossCalculator,
        FundingRateHandler,
        FundingCalculator,
        LiquidationCalculator,
        InterestCalculator,
        HealthFactorCalculator,
    )

    # IL calculation
    calc = ImpermanentLossCalculator()
    il_pct, token0_amt, token1_amt = calc.calculate_il_v3(
        entry_price=Decimal("2000"),
        current_price=Decimal("2200"),
        tick_lower=-887220,
        tick_upper=887220,
        liquidity=Decimal("1000000"),
    )

    # Funding rate calculation (index-based)
    handler = FundingRateHandler()
    payment = handler.calculate_funding_payment(
        position=perp_long_position,
        current_funding_index=Decimal("0.0015"),
        position_value_usd=Decimal("10000"),
    )

    # Funding calculation (time-based, preferred for mark_to_market)
    calculator = FundingCalculator()
    result = calculator.calculate_funding_payment(
        position=perp_long_position,
        funding_rate=Decimal("0.0001"),
        time_delta_hours=Decimal("24"),
    )

    # Liquidation price calculation
    liq_calc = LiquidationCalculator()
    liq_price = liq_calc.calculate_liquidation_price(
        entry_price=Decimal("2000"),
        leverage=Decimal("5"),
        maintenance_margin=Decimal("0.05"),
        is_long=True,
    )

    # Interest calculation for lending positions
    interest_calc = InterestCalculator()
    result = interest_calc.calculate_interest(
        principal=Decimal("10000"),
        apy=Decimal("0.05"),  # 5% APY
        time_delta=Decimal("30"),  # 30 days
        compound=True,
    )

    # Health factor calculation for lending positions
    hf_calc = HealthFactorCalculator()
    hf_result = hf_calc.calculate_health_factor(
        collateral_value_usd=Decimal("10000"),
        debt_value_usd=Decimal("6000"),
        liquidation_threshold=Decimal("0.825"),
    )
    # hf_result.health_factor = 1.375

    # Benchmark metrics calculation
    from almanak.framework.backtesting.pnl.calculators import (
        BenchmarkCalculator,
        calculate_information_ratio,
        calculate_beta,
        calculate_alpha,
    )

    bench_calc = BenchmarkCalculator()
    ir = bench_calc.calculate_information_ratio(strategy_rets, benchmark_rets)
    beta = bench_calc.calculate_beta(strategy_rets, benchmark_rets)
    alpha = bench_calc.calculate_alpha(
        strategy_return=Decimal("0.15"),
        benchmark_return=Decimal("0.10"),
        beta=beta,
        risk_free_rate=Decimal("0.05"),
    )
"""

from almanak.framework.backtesting.pnl.calculators.attribution import (
    AttributionCalculator,
    AttributionResult,
    attribute_pnl_by_asset,
    attribute_pnl_by_intent_type,
    attribute_pnl_by_protocol,
    calculate_all_attributions,
    verify_attribution_totals,
)
from almanak.framework.backtesting.pnl.calculators.benchmark import (
    BenchmarkCalculator,
    calculate_alpha,
    calculate_beta,
    calculate_information_ratio,
)
from almanak.framework.backtesting.pnl.calculators.funding import (
    FundingCalculator,
    FundingPaymentResult,
    FundingRateHandler,
    FundingRateSource,
)
from almanak.framework.backtesting.pnl.calculators.health_factor import (
    HealthFactorCalculator,
    HealthFactorResult,
    HealthFactorWarning,
)
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.calculators.interest import (
    InterestCalculator,
    InterestRateSource,
    InterestResult,
)
from almanak.framework.backtesting.pnl.calculators.liquidation import (
    LiquidationCalculator,
    LiquidationWarning,
)
from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
    LiquidationParamRegistry,
    LiquidationParams,
    LiquidationParamSource,
)
from almanak.framework.backtesting.pnl.calculators.margin import (
    MarginUtilization,
    MarginValidationResult,
    MarginValidator,
)
from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
    MonteCarloPathGenerator,
    PathGenerationMethod,
    PricePathConfig,
    PricePathResult,
    generate_price_paths,
)
from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
    MonteCarloConfig,
    MonteCarloPathBacktestResult,
    MonteCarloSimulationResult,
    SimulatedPricePathProvider,
    run_monte_carlo,
    run_monte_carlo_sync,
)

__all__ = [
    # Benchmark metrics
    "BenchmarkCalculator",
    "calculate_information_ratio",
    "calculate_beta",
    "calculate_alpha",
    # Funding
    "FundingCalculator",
    "FundingPaymentResult",
    "FundingRateHandler",
    "FundingRateSource",
    "HealthFactorCalculator",
    "HealthFactorResult",
    "HealthFactorWarning",
    "ImpermanentLossCalculator",
    "InterestCalculator",
    "InterestRateSource",
    "InterestResult",
    "LiquidationCalculator",
    "LiquidationWarning",
    "LiquidationParamRegistry",
    "LiquidationParams",
    "LiquidationParamSource",
    "MarginUtilization",
    "MarginValidationResult",
    "MarginValidator",
    "MonteCarloPathGenerator",
    "PathGenerationMethod",
    "PricePathConfig",
    "PricePathResult",
    "generate_price_paths",
    # Monte Carlo runner
    "MonteCarloConfig",
    "MonteCarloPathBacktestResult",
    "MonteCarloSimulationResult",
    "SimulatedPricePathProvider",
    "run_monte_carlo",
    "run_monte_carlo_sync",
    # Attribution
    "AttributionCalculator",
    "AttributionResult",
    "attribute_pnl_by_protocol",
    "attribute_pnl_by_intent_type",
    "attribute_pnl_by_asset",
    "verify_attribution_totals",
    "calculate_all_attributions",
]
