# E2E Strategy Test Report: spark_lender (Anvil)

**Date:** 2026-02-15 06:36
**Result:** PASS
**Mode:** Anvil
**Duration:** ~1 minute

## Configuration

| Field | Value |
|-------|-------|
| Strategy | spark_lender |
| Chain | ethereum |
| Network | Anvil fork |
| Anvil Port | 55669 (managed gateway) |

## Execution

### Setup
- [x] Managed gateway auto-started Anvil on port 55669
- [x] Wallet auto-funded with USDC and WETH
- [x] **DAI manually funded via storage slot** (100 DAI) - auto-funding failed for DAI

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Intents executed: SUPPLY (5 DAI to Spark)

### Initial Attempt
- Transaction 1 (approval): SUCCESS - tx hash `5081d79faca3c6bc36ba3b510129a34e8e37a562d8d23e27bca71b96b859b80c`
- Transaction 2 (supply): FAILED - reverted with "Invalid revert data (too short): 0x"

### Retry Attempt (1/3)
- Transaction (supply): SUCCESS - tx hash `21ed6b93153e80c3fdba7351cd165b94f8754ac5d0ed00947c36e9a1dfdc0643`
- Gas used: 200,539
- Block: 24460450

### Key Log Output
```text
[2026-02-15T06:36:09.209832Z] [info] ✅ EXECUTED: SUPPLY completed successfully
[2026-02-15T06:36:09.209935Z] [info]    Txs: 1 (21ed6b...0643) | 200,539 gas
[2026-02-15T06:36:09.210503Z] [info] Execution successful for SparkLenderStrategy: gas_used=200539, tx_count=1
[2026-02-15T06:36:09.210793Z] [info] Parsed Spark receipt: tx=..., supplies=1, withdraws=0, borrows=0, repays=0
[2026-02-15T06:36:09.210878Z] [info] Enriched SUPPLY result with: supply_amount, a_token_received (protocol=spark, chain=ethereum)
[2026-02-15T06:36:09.217198Z] [info] Supply successful: 5 DAI -> Spark

Status: SUCCESS | Intent: SUPPLY | Gas used: 200539 | Duration: 34176ms
```

## Result

**PASS** - Strategy successfully supplied 5 DAI to Spark protocol after one retry. The initial attempt's supply transaction reverted (likely a nonce or timing issue), but the retry succeeded on the first attempt with the approval already confirmed.

## Notes

1. **DAI Funding Issue**: The managed gateway's auto-funding failed to fund DAI initially:
   ```
   [error] Failed to fund wallet 0xf39Fd6e5...
   ```
   This was resolved by manually funding DAI via storage slot 2 on the Anvil fork:
   ```bash
   cast rpc anvil_setStorageAt $DAI $SLOT 0x0000000000000000000000000000000000000000000000056BC75E2D63100000
   ```

2. **Transaction Count**: The strategy compiled to 2 transactions initially (approve + supply), but after the approval succeeded in the first attempt, the retry only needed 1 transaction (supply).

3. **Gas Estimation Warning**: Gas estimation failed on the first attempt with "Dai/insufficient-balance" error, which was misleading since the wallet was being funded concurrently.

4. **Receipt Parsing**: The Spark receipt parser successfully extracted supply data:
   - supplies=1
   - withdraws=0
   - borrows=0
   - repays=0

5. **Result Enrichment**: The framework automatically enriched the result with:
   - `supply_amount`
   - `a_token_received`

## Recommendations

1. Add DAI to the auto-funding token list for Ethereum Anvil forks
2. Investigate the initial supply transaction revert (may be related to timing/nonce handling)
3. The retry mechanism worked correctly - this demonstrates the framework's resilience
