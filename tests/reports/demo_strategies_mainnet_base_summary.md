# Demo Strategies - Base Chain Mainnet Summary

**Date**: 2026-02-26
**Network**: Mainnet (Base chain only)
**Wallet**: `0x0738Ea642faA28fFc588717625e45F3078fDBAC9`
**Total Strategies Tested**: 3 (filtered from 13 total demo strategies)
**Skipped**: 10 strategies (non-Base chains: arbitrum, ethereum, avalanche, plasma)

---

## Strategy Results

| # | Strategy | Chain | Funded | Status | Suspicious | TX Hash | Notes |
|---|----------|-------|--------|--------|------------|---------|-------|
| 1 | aerodrome_lp | base | YES (Method A: wrap ETH) | PASS | 5 (0 err) | [0x96d47a5...](https://basescan.org/tx/0x96d47a528ff0aa1fbb9c3ad8fa39e7a4aa6fb190062d3d48f93b6418eb069807) | 3 TXs (2 approvals + 1 add liquidity). Teardown did NOT close the LP position. |
| 2 | almanak_rsi | base | FAILED (Enso rate limit) | FAIL | 5 (1 err) | 4 reverted TXs | STF error - insufficient USDC ($1.07 available, $2.00 needed) |
| 3 | enso_rsi | base | SKIPPED (adjusted config) | FAIL | 3 (1 err) | - | Missing ENSO_API_KEY env var. No TXs submitted. |

**Tally**: 1 PASS / 0 PASS(HOLD) / 0 PARTIAL / 2 FAIL out of 3 total

---

## Portfolio Before/After Comparison

### Token Balances

| Token | BEFORE | AFTER | Delta |
|-------|--------|-------|-------|
| ETH | 0.000299 | 0.000205 | -0.000094 (gas) |
| WETH | 0.000559 | 0.000629 | +0.000070 (wrapped for LP) |
| USDC | 1.112384 | 1.072384 | -0.040000 (LP deposit) |
| USDT | 0.07 | 0.07 | 0 |
| ALMANAK | 4535.73 | 4535.73 | 0 |
| vAMM-WETH/USDC | 8.63e-10 | 1.72e-09 | +8.6e-10 (LP added) |

### DeFi Positions

| Protocol | Type | BEFORE Value | AFTER Value | Status |
|----------|------|-------------|------------|--------|
| Aerodrome | LP (WETH/USDC vAMM) | $0.08 | $0.16 | NOT torn down |

No new lending, staking, or borrowing positions were created.

---

## Teardown Assessment

**Question**: Did the strategies tear down their resources?

**Answer**: **No - partial teardown failure.**

The `aerodrome_lp` strategy was run with `--teardown-after` but the Aerodrome LP position was **not closed**. The LP token balance approximately doubled (8.63e-10 -> 1.72e-09), confirming the LP_OPEN executed successfully but the subsequent teardown (LP_CLOSE) either did not fire or failed silently.

- No "funny positions" in lending or staking protocols were found
- The only residual position is a tiny Aerodrome LP ($0.16 of dust)
- The other two strategies (almanak_rsi, enso_rsi) both failed before creating any DeFi positions

**Root cause to investigate**: The `--teardown-after` flag did not successfully close the Aerodrome volatile AMM LP position. This may be a bug in the teardown logic for Aerodrome vAMM pools.

---

## Suspicious Behaviour Summary

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| aerodrome_lp | 5 | 0 | Token resolution errors (wrong chain), gas estimate warnings, insecure mode |
| almanak_rsi | 5 | 1 | STF reverts (insufficient balance), token resolution errors |
| enso_rsi | 3 | 1 | Missing Enso API key, placeholder prices, insecure mode |

- Total suspicious findings: 13
- Strategies with ERROR-level findings: 2
- Strategies with clean logs: 0
- Most common patterns: token resolution errors (benign, cross-chain init), insecure mode warning, insufficient balance

---

## Funding Audit

- Strategies needing funding: 2 (aerodrome_lp, almanak_rsi)
- Successfully funded: 1 (aerodrome_lp - wrapped ETH to WETH)
- Funding failed: 1 (almanak_rsi - Enso API rate limited)
- Total funding TXs: 2 (1 wrap + 1 Permit2 approval)
- Gas spent on funding: minimal (Base L2 fees)
