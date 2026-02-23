# E2E Strategy Test Report: traderjoe_vol_rebalancer (Anvil)

**Date:** 2026-02-20 16:21
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | incubating_traderjoe_vol_rebalancer |
| Chain | avalanche |
| Network | Anvil fork (Avalanche mainnet, chain ID 43114) |
| Anvil Port | 58051 (managed) |
| Pool | WAVAX/USDC/20 |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `capital_y` | `"30"` | `"20"` | Budget cap: 1.0 WAVAX (~$9.30) + 30 USDC = ~$39 was OK, but reduced to $20 for extra margin |
| `force_action` | (not present) | `"open"` | Force an immediate LP_OPEN instead of waiting for ATR signal |

Note: At the time of the run, WAVAX was priced at $9.30/AVAX. 1.0 WAVAX + 20 USDC = ~$29.30, well under the $50 cap.
After the test, both changes were reverted: `capital_y` restored to `"30"` and `force_action` removed.

## Execution

### Setup
- Anvil fork of Avalanche mainnet started on port 58051 (managed by the CLI)
- Managed gateway auto-started on 127.0.0.1:50052 (insecure mode, Anvil network)
- Wallet funded: 100 AVAX native + 100 WAVAX (via storage slot 3) + 10,000 USDC (via storage slot 9) by managed gateway

### Strategy Decision
- WAVAX price: $9.30 | USDC price: $0.9999
- `force_action=open` triggered immediate LP_OPEN (bypassed ATR volatility classification)
- Volatility regime: `medium` (default, no ATR computed due to forced action)
- LP range: [8.8358, 9.7658] WAVAX/USDC (10% width centered at current price)
- Capital deployed: 1.0 WAVAX + 20 USDC

### Intent Executed
- LP_OPEN via TraderJoe V2, pool WAVAX/USDC bin_step=20
- Compiled to 3 transactions (approval x2 + addLiquidity)

### Transactions

| # | TX Hash | Block | Gas Used | Status |
|---|---------|-------|----------|--------|
| 1 (Approve WAVAX) | `d732a1768e21e15549d5eedc27c02ca379184f29fce26197d9aed381c280761b` | 78532595 | 46,135 | SUCCESS |
| 2 (Approve USDC) | `f5aaacb067b1394bf50ed92c285c1137a5a6e1fa084dc52e397845c020c6b8aa` | 78532596 | 55,449 | SUCCESS |
| 3 (addLiquidity) | `65abae3c8fb1c1a0f30fd70065538fa12bd5303e5808e398f5f7c379248b2a4e` | 78532597 | 598,626 | SUCCESS |

**Total gas used:** 700,210

### Key Log Output

```text
TJVolRebalancer initialized: pool=WAVAX/USDC/20, regime=medium, range=10.00%, position=NO
Aggregated price for WAVAX/USD: 9.3 (confidence: 1.00, sources: 1/1, outliers: 0)
Aggregated price for USDC/USD: 0.999913 (confidence: 1.00, sources: 1/1, outliers: 0)
LP_OPEN: 1.0 WAVAX + 20 USDC, range=[8.8358, 9.7658], regime=medium, width=10.00%
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs, 860000 gas
Transaction submitted: tx_hash=d732a1...761b
Transaction submitted: tx_hash=f5aaac...b8aa
Transaction submitted: tx_hash=65abae...2a4e
Transaction confirmed: d732a1... block=78532595, gas_used=46135
Transaction confirmed: f5aaac... block=78532596, gas_used=55449
Transaction confirmed: 65abae... block=78532597, gas_used=598626
EXECUTED: LP_OPEN completed successfully | Txs: 3 | 700,210 gas
LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 700210 | Duration: 25758ms
Iteration completed successfully.
```

### Observations

- Receipt enrichment ran successfully: `Enriched LP_OPEN result with: bin_ids (protocol=traderjoe_v2, chain=avalanche)`
- A non-critical warning appeared: `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` -- this is benign for LP_OPEN (no chained amounts used).
- Gas estimation on tx3 (addLiquidity) fell back to compiler-provided limit (gas estimation failure on Anvil fork for TraderJoe calldata is a known pattern).

## Result

**PASS** - Strategy produced 3 on-chain transactions on the Avalanche Anvil fork, successfully opening a TraderJoe V2 LP position in the WAVAX/USDC 20bps pool at a 10% width (medium volatility regime) around the current price of $9.30.
