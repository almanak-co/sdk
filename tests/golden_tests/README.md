# Golden Test Fixtures

This directory contains known-good test fixtures for regression testing of the backtesting system. Each fixture includes documented ground truth sources and tolerance thresholds for validation.

## Purpose

Golden tests serve as accuracy benchmarks to detect regressions in the backtesting calculations. Unlike unit tests that verify behavior, golden tests compare outputs against known historical outcomes.

## Fixture Categories

### LP Positions (`lp_fixtures.json`)

Three LP position fixtures covering different scenarios:

| ID | Scenario | IL Tolerance | Fees Tolerance |
|----|----------|--------------|----------------|
| Q4_2024_LP_001 | Full-range bull market | +/-5% | +/-10% |
| Q4_2024_LP_002 | Concentrated sideways | +/-5% | +/-10% |
| Q4_2024_LP_003 | Narrow range breakout | +/-5% | +/-15% |

**Ground Truth Sources:**
- IL calculations: `ImpermanentLossCalculator.calculate_il_v3()` using V3 concentrated liquidity math
- Fee estimates: Standard V3 fee model based on volume share and fee tier
- Price data: Q4 2024 ETH/USDC historical prices (CoinGecko verified)
- Reference pool: Uniswap V3 ETH/USDC 0.3% (0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8)

### Perp Trades (`perp_fixtures.json`)

Three perp trade fixtures covering different scenarios:

| ID | Scenario | Funding Tolerance | PnL Tolerance |
|----|----------|-------------------|---------------|
| DEC_2024_PERP_001 | 5x Long bull market | +/-10% | +/-5% |
| DEC_2024_PERP_002 | 3x Short bear market | +/-10% | +/-5% |
| DEC_2024_PERP_003 | 10x Long high funding | +/-10% | +/-5% |

**Ground Truth Sources:**
- Funding calculations: `FundingCalculator` using GMX V2 hourly funding model
- PnL calculations: Standard perp PnL formula (size * price_change_pct * direction)
- Price data: December 2024 ETH/USD prices from GMX V2 historical data
- Reference protocol: GMX V2 ETH-USD market on Arbitrum

### Lending Positions (`lending_fixtures.json`)

Two lending position fixtures covering supply and borrow:

| ID | Scenario | Interest Tolerance | HF Tolerance |
|----|----------|-------------------|--------------|
| AAVE_2024_SUPPLY_001 | 6-month USDC supply | +/-2% | N/A |
| AAVE_2024_BORROW_001 | 3-month USDC borrow | +/-2% | +/-5% |

**Ground Truth Sources:**
- Interest calculations: `InterestCalculator` with compound interest formula (daily compounding)
- Health factor: `HealthFactorCalculator` using Aave V3 formula
- Rate data: 2024 Aave V3 historical supply/borrow rates
- Reference protocol: Aave V3 on Ethereum mainnet

## Tolerance Rationale

### IL Tolerance (5%)
IL calculation accuracy depends on:
- Price precision at entry/exit
- Tick calculation accuracy
- Liquidity distribution assumptions

5% tolerance accounts for minor numerical precision differences while catching significant algorithmic errors.

### Funding Tolerance (10%)
Funding rate calculations are subject to:
- Time interval rounding
- Rate averaging methodology
- Protocol-specific adjustments

10% tolerance is appropriate for the inherent variability in funding rate estimation.

### Interest Tolerance (2%)
Interest accrual is highly deterministic:
- Uses standard compound interest formula
- No external data dependencies
- Minimal rounding errors

2% tolerance is strict but achievable for pure mathematical calculations.

### Fee Tolerance (10-15%)
Fee estimation has highest uncertainty:
- Depends on volume assumptions
- Pool liquidity share varies
- In-range time estimation for concentrated positions

Higher tolerance (15% for out-of-range scenarios) accounts for estimation uncertainty.

## Usage

```python
from tests.golden_tests import load_lp_fixtures, load_perp_fixtures, load_lending_fixtures

# Load fixtures
lp_data = load_lp_fixtures()
perp_data = load_perp_fixtures()
lending_data = load_lending_fixtures()

# Access individual fixtures
for fixture in lp_data["fixtures"]:
    print(f"Testing {fixture['id']}: {fixture['description']}")
    # Run backtest with fixture['input']
    # Compare results to fixture['expected']
    # Use fixture['tolerances'] for assertions
```

## Validation Tests

Golden test validation is implemented in `test_golden_accuracy.py` (US-089b).

## Updating Fixtures

When updating fixtures:
1. Document the reason for the change
2. Update the `metadata.version` field
3. Verify ground truth source is still valid
4. Run validation tests to confirm accuracy

## Related Documentation

- `tests/validation/backtesting/test_lp_historical_accuracy.py` - LP validation tests
- `tests/validation/backtesting/test_perp_historical_accuracy.py` - Perp validation tests
- `tests/validation/backtesting/test_lending_historical_accuracy.py` - Lending validation tests
- [CONTRIBUTING.md](../../CONTRIBUTING.md) - Contributing and testing guidelines
