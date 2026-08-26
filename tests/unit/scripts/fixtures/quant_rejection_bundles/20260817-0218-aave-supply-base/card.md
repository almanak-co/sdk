# Test Card: Aave V3 supply lifecycle on Base Mainnet
MODE: certify
NETWORK: mainnet
GOAL: action-density
STRATEGY_DIR: docs/internal/quant-user-runs/_batches/20260817-0218-quant4-mainnet/aave-supply-base-strategy
BATCH: 20260817-0218-quant4-mainnet
TIME_BUDGET_MIN: 10
TRADING_CAP_USD: 4.00
GAS_BUDGET_USD: 1.00
TOTAL_WALLET_CAP_USD: 5.00
PNL_TOLERANCE: 0.50 or 5%
WALLET: durable pool #1
CHAIN: base
GATEWAY_PORT: 50361
DASHBOARD_PORT: 8561
TESTED_SHA: 1a62591298c4b009aafba6190de440a003196cc5

Card consistency: 4.00 + 1.00 = 5.00 <= 5.00. Runs sequentially; no overlapping funded leg.

## Assertions

A_act. Exactly one runtime SUPPLY must execute before the hold; HOLD is not coverage.
A1. The SUPPLY receipt is status 1 and bijectively bound to the submitted transaction.
A2. Aave reserve state at a pinned post-supply block proves positive USDC aToken principal for this wallet.
A3. Accounting persists the SUPPLY event with measured principal and preserves Empty != Zero.
A4. A separate graceful teardown executes WITHDRAW and reaches acknowledged -> started -> completed.
A5. Connector-native terminal proof at a pinned block shows USDC aToken balance zero.
A6. Dashboard early, pre-teardown, and post-teardown evidence agrees with receipts, SQLite, and terminal state.
A7. Wallet/component reconciliation gap is <= $0.50 or 5%, with gas included and no unexplained capital flow.
A_cap. Whole-wallet value stays <= $5.00 at funding and throughout the run; sweep releases the durable wallet.

## Declared preparation

The copied config changes supply_amount from 100 to 3.6 and force_action from "supply" to "". This preserves the strategy law while preventing forced repeated supplies. No production source is modified.
