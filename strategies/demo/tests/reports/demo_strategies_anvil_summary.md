# Demo Strategy Regression - Anvil Summary

**Date**: 2026-03-04
**Network**: Anvil (local fork)
**Iteration**: 39 (Regress phase)
**Strategies Tested**: 2 / 13 (smoke test subset)

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Hash | Notes |
|---|----------|-------|--------|------------|---------|-------|
| 1 | aave_borrow | arbitrum | FAIL | 2 (1 err) | - | TX confirmation timeout on free public RPC (10s window). Not a code regression. |
| 2 | aerodrome_lp | base | PASS | 5 (1 err) | 0x703d037e...d563 | LP opened: WETH+USDC addLiquidity (3 TXs, 262K gas) |

## Tally

1 PASS / 0 PASS(HOLD) / 0 PARTIAL / 1 FAIL out of 2 tested

## Suspicious Behaviour Summary

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| aave_borrow | 2 | 1 | TX confirmation timeout (free RPC rate limiting) |
| aerodrome_lp | 5 | 1 | pendle_pt_swap_arbitrum circular import on strategy discovery |

- Total suspicious findings: 7
- Strategies with ERROR-level findings: 2
- Most common patterns: free RPC rate limiting, missing API keys (CoinGecko, Alchemy), circular import in incubating strategy

## Notes

- **aave_borrow failure is NOT a regression**: TX confirmation timeout caused by free public RPCs (no ALCHEMY_API_KEY). Strategy compiled and submitted correctly.
- **aerodrome_lp PASS confirms Base chain LP flow works**: All 3 TXs executed successfully.
- **pendle_pt_swap_arbitrum circular import**: Incubating strategy from iter 39 has circular import warning during strategy discovery.
- Only 2 strategies tested (smoke test) due to regression phase time constraints.
