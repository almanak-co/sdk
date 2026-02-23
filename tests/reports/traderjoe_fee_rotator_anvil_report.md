# E2E Strategy Test Report: traderjoe_fee_rotator (Anvil)

**Date:** 2026-02-20 09:18
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | incubating_traderjoe_fee_rotator |
| Chain | avalanche (Chain ID 43114) |
| Network | Anvil fork (managed, port auto-assigned ~57477) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

The original config was modified for this test and restored afterward.

| Field | Original | Test Value | Reason |
|-------|----------|------------|--------|
| `pool_a_wavax` | 1.0 | 0.5 | Budget cap ($50 max per trade) |
| `pool_a_usdc` | 30 | 15 | Budget cap |
| `pool_b_wavax` | 1.0 | 0.5 | Budget cap |
| `pool_b_weth_e` | 0.01 | 0.005 | Budget cap |
| `swap_rotation_usdc` | 20 | 10 | Budget cap |
| `force_action` | (absent) | "open_a" | Force immediate LP open (strategy supports this) |

Total test position value: ~0.5 WAVAX (~$4.65) + 15 USDC + 0.5 WAVAX (~$4.65) + 0.005 WETH.e (~$9.85) = ~$34.15 (under $50 cap).

Config was fully restored to original values after the test.

## Execution

### Setup
- Managed gateway auto-started (no pre-existing gateway needed - strategy CLI handles it)
- Managed Anvil fork auto-started on port 57477 (Avalanche mainnet fork, block 78532400)
- Wallet auto-funded by managed gateway: 100 AVAX, 100 WAVAX, 10,000 USDC, 1 WETH.e
- Prices fetched from CoinGecko: WAVAX=$9.29, USDC=$1.00, WETH.e=$1968.98

### Strategy Run
- `force_action: "open_a"` triggered an immediate LP_OPEN on Pool A (WAVAX/USDC/20)
- Strategy computed range: [8.8256, 9.7546] USDC per WAVAX (10% width around spot $9.29)
- Compiled to 3 transactions (approve WAVAX, approve USDC, addLiquidity)
- All 3 transactions confirmed on-chain

### Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve WAVAX | `0xa2cb2ee822ccef8907ed62224f8285745076ad2c853359a4c1a34cb229205fdd` | 78532404 | 46,135 | SUCCESS |
| Approve USDC | `0xada964db8bd07a8b0275db5f7ff3a91210b114d66d661d5515e0966500445c2b` | 78532405 | 55,437 | SUCCESS |
| addLiquidity | `0x709ff7db26dbe4ef716ca02f9abd25ae491a0d59ce2ac723d7008682dece5d34` | 78532406 | 598,286 | SUCCESS |

**Total gas used: 699,858**

### Post-execution State
- LP position opened in Pool A (WAVAX/USDC/20)
- Result enricher extracted bin_ids: 11 bins active in Pool A
- Phase transitioned: INIT -> OPENING_B (next run would open Pool B)
- FSM state persisted to `almanak_state.db`

### Key Log Output

```text
LP_OPEN Pool A: 0.5 WAVAX + 15 USDC, range=[8.8256, 9.7546], width=10.00%
Intent: LP_OPEN: WAVAX/USDC/20 (0.5, 15) [9 - 10] via traderjoe_v2
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs, 860000 gas
EXECUTED: LP_OPEN completed successfully
Txs: 3 (a2cb2e...5fdd, ada964...5c2b, 709ff7...5d34) | 699,858 gas
Pool A opened: 11 bins
Status: SUCCESS | Intent: LP_OPEN | Gas used: 699858 | Duration: 25956ms
Iteration completed successfully.
```

### Non-critical Warning
```text
Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail
```
This is a non-blocking informational warning about amount chaining for multi-step intents. LP_OPEN only has one logical intent (3 txs), so this does not affect correctness.

## Result

**PASS** - Strategy executed an LP_OPEN on TraderJoe V2 WAVAX/USDC/20 pool (Avalanche Anvil fork), confirmed 3 on-chain transactions totaling 699,858 gas, and correctly advanced the FSM phase from INIT to OPENING_B.
