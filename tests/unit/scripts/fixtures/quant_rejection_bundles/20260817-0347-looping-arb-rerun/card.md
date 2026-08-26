# Test Card: Aave V3 looping lifecycle on Arbitrum Mainnet
MODE: certify
NETWORK: mainnet
GOAL: action-density
STRATEGY_DIR: docs/internal/quant-user-runs/_batches/20260817-0218-quant4-mainnet/looping-arb-strategy
BATCH: 20260817-0218-quant4-mainnet
TIME_BUDGET_MIN: 10
TRADING_CAP_USD: 4.00
GAS_BUDGET_USD: 1.00
TOTAL_WALLET_CAP_USD: 5.00
PNL_TOLERANCE: 0.50 or 5%
WALLET: durable pool #12
CHAIN: arbitrum
GATEWAY_PORT: 50364
DASHBOARD_PORT: 8564
TESTED_SHA: 1a62591298c4b009aafba6190de440a003196cc5

Card consistency: 4.00 + 1.00 = 5.00 <= 5.00. Runs sequentially; no overlapping funded leg.

## Assertions

A_act. Runtime executes SUPPLY -> BORROW -> SWAP -> SUPPLY -> BORROW before HOLD.
A1. Exact Aave Pool/reserve and Uniswap pool bindings are proven; every receipt is status 1 and bijective.
A2. Pinned pre-teardown Aave reads prove positive USDC collateral, positive USDT variable debt, and HF > 1.5.
A3. Separate graceful teardown executes at least one REPAY and WITHDRAW, with SWAP when the live plan requires it.
A4. Every withdrawal step preserves the declared HF safety floor; every swap has measured production guards.
A5. Pinned terminal Aave state proves aToken balance and total debt are zero; wallet is consolidated to USDC.
A6. Accounting covers every successful intent and preserves Empty != Zero.
A7. All dashboard phases reconcile collateral, debt, HF, action sequence, balances, and PnL sign.
A_cap. Whole-wallet value stays <= $5.00; wallet is swept and released only after terminal proof.
