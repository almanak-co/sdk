# Reference Strategies

Production-quality reference implementations demonstrating advanced DeFi strategy
patterns built with the Almanak SDK. These strategies showcase the framework's
most sophisticated features: multi-protocol composition, atomic flash loans,
cross-protocol yield optimization, and dynamic risk management.

> **Disclaimer**: These are reference implementations for educational and
> demonstration purposes. They are not financial advice. Use at your own risk.

## Strategies

### 1. ethena_pt_leverage -- Leveraged Fixed Yield via Pendle PT + Morpho Blue Flash Loans

**Chain**: Ethereum | **Protocols**: Pendle, Morpho Blue, Enso

Locks in fixed yield on Pendle PT-sUSDe, then amplifies with 3x leverage using
atomic flash loan entry/exit. Entry and exit are single-transaction operations --
if any step fails, the entire transaction reverts.

**SDK patterns demonstrated**: `FlashLoanIntent` with nested callbacks,
`build_pt_leverage_loop` factory, maturity-aware monitoring, projected health
factor checks.

**Note**: Requires a Safe (smart contract) wallet. Flash loans need a receiver
contract that implements the provider callback. EOA wallets cannot execute this
strategy. Run with `--network anvil` using a Safe-enabled configuration.

```bash
# Requires Safe wallet -- will fail gracefully with EOA
almanak strat run -d strategies/reference/ethena_pt_leverage --fresh --once --network anvil
```

### 2. ethena_leverage_loop -- Recursive sUSDe Yield Amplification via Morpho Blue

**Chain**: Ethereum | **Protocols**: Ethena, Morpho Blue, Enso

Amplifies Ethena sUSDe staking yield (~10%) by recursively borrowing USDC (~6%)
against sUSDe collateral on Morpho Blue. At 2x leverage, targets ~14% net APY
from the structural yield spread.

**SDK patterns demonstrated**: 8-phase state machine (idle -> setup -> loop ->
monitor), single-intent-per-step progression, health factor monitoring with
auto-deleveraging, receipt-based state reconciliation.

```bash
# Run first step (swap USDC -> USDe)
almanak strat run -d strategies/reference/ethena_leverage_loop --fresh --once --network anvil

# Run full lifecycle
almanak strat run -d strategies/reference/ethena_leverage_loop --fresh --interval 15 --network anvil
```

### 3. morpho_aave_arb -- Cross-Protocol Yield Rotation (Morpho Blue vs Aave V3)

**Chain**: Ethereum | **Protocols**: Morpho Blue, Aave V3

Monitors wstETH supply rates across Morpho Blue and Aave V3, rotating capital to
whichever protocol currently offers the better rate. Uses `IntentSequence`
(withdraw -> supply) for sequential rebalancing with dynamic gas-aware spread
thresholds and a circuit breaker for consecutive unprofitable rotations.

**SDK patterns demonstrated**: `Intent.sequence()` for multi-step operations,
cross-protocol yield comparison, dynamic threshold calculation, circuit breaker
pattern, fail-fast on missing data (never falls back to hardcoded APYs).

```bash
# Deploy to best protocol
almanak strat run -d strategies/reference/morpho_aave_arb --fresh --once --network anvil

# Run continuously to monitor and rebalance
almanak strat run -d strategies/reference/morpho_aave_arb --fresh --interval 30 --network anvil
```
