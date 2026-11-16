# Aave V3 Avalanche lending

> Aave V3 supply-and-borrow lifecycle on Avalanche: supply WAVAX, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

Reference using explicitly configured WAVAX collateral and native USDC debt on Avalanche C-Chain. The actual strategy exercises supply, partial and full withdrawal, collateral control, borrow, partial and full repayment, hold, restart and cleanup. Five WAVAX fund five USDC of requested debt. The operator fork fixture also funds one USDC so the fund can repay accrued interest; repayment needs no configured amount.

The configured oracle sources and revision-11 fingerprints were measured at block 94824453. Prepared execution uses the explicit economic configuration, the author decision interval, one-second eligibility latency and excluded costs. Six native lifecycles, four actual process cases, isolated prepared recovery and CLI/report/dashboard checks were verified. `almanak support --connector aave_v3 --chain eip155:43114` states the declaration; fork proofs use simulated funds.
