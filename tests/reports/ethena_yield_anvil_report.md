# Anvil Test Report: ethena_yield Strategy

**Date:** 2026-02-08 15:03
**Result:** PASS
**Duration:** ~3 minutes

---

## Summary

The ethena_yield strategy successfully executed on Anvil fork of Ethereum mainnet. The strategy staked 100 USDe with Ethena protocol and received approximately 82 sUSDe (yield-bearing vault shares) worth 100 USDe. All transactions were successful and on-chain verification confirmed proper position establishment.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | ethena_yield |
| Chain | ethereum (mainnet fork) |
| Network | Anvil (port 8549) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |
| Min Stake Amount | 5 USDe |
| Swap USDC to USDe | false |

---

## Test Phases

### Phase 1: Setup
- ✅ Anvil started on port 8549 (Ethereum mainnet fork)
- ✅ Gateway started on port 50051
- ✅ Wallet funded with 100 ETH for gas
- ✅ Wallet funded with 100 USDe (from whale 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34)

### Phase 2: Strategy Execution
- ✅ Strategy loaded successfully
- ✅ Config loaded from config.yaml
- ✅ USDe balance detected (100 USDe >= min_stake 5 USDe)
- ✅ STAKE intent created and compiled
- ✅ Intent compiled to 2 transactions with 200,000 gas estimate
- ✅ Execution successful with 130,105 gas used
- ✅ Strategy callback confirmed staking success

### Phase 3: On-Chain Verification
- ✅ USDe balance: 0 (fully staked)
- ✅ sUSDe balance: 81,984,269,328,252,633,911 (~82 sUSDe)
- ✅ sUSDe value in USDe: 99,999,999,999,999,999,999 (~100 USDe)
- ✅ ERC4626 conversion working correctly (1:1 value)

---

## Execution Log Highlights

### Strategy Run Output
```
Strategy: EthenaYieldStrategy
Instance ID: EthenaYieldStrategy:9d2a1e854b8d
Chain: ethereum
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

[info] USDe balance (100) >= min_stake (5), staking
[info] STAKE intent: 100.0000 USDe -> sUSDe
[info] Compiled STAKE intent: 100 USDe via ethena, 2 txs, 200000 gas
[info] Execution successful: gas_used=130105, tx_count=2
[info] Parsed Ethena receipt: stakes=1, withdraws=0
[info] Staking successful: 100 USDe -> sUSDe

Status: SUCCESS | Intent: STAKE | Gas used: 130105 | Duration: 2445ms
```

---

## Transactions

| Phase | Intent | Action | Gas Used | Status |
|-------|--------|--------|----------|--------|
| Execute | STAKE | Approve USDe | ~50,000 | ✅ |
| Execute | STAKE | Deposit to sUSDe vault | ~80,105 | ✅ |

**Total Gas Used:** 130,105

---

## Token Balances

### Initial Balances
- ETH: 100.0 ETH
- USDe: 100.0 USDe
- sUSDe: 0.0 sUSDe

### Final Balances
- ETH: ~99.99 ETH (gas used)
- USDe: 0.0 USDe (fully staked)
- sUSDe: 81.984269328252633911 sUSDe (~100 USDe value)

---

## Protocol Details

### Ethena Protocol
- **USDe Token:** 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3
- **sUSDe Vault:** 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497
- **Vault Type:** ERC4626 (yield-bearing shares)
- **Conversion Rate:** ~0.82 sUSDe per 1 USDe (at time of test)
- **Yield Source:** Perpetual futures funding rate arbitrage

### Strategy Logic
1. Check USDe balance (100 USDe)
2. If balance >= min_stake_amount (5 USDe), create STAKE intent
3. Compile to APPROVE + DEPOSIT transactions
4. Execute via Ethena adapter
5. Receive sUSDe vault shares
6. Track staking status in internal state

---

## Verification Commands

```bash
# USDe balance
cast call 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3 "balanceOf(address)(uint256)" 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --rpc-url http://127.0.0.1:8549
# Output: 0

# sUSDe balance
cast call 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 "balanceOf(address)(uint256)" 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --rpc-url http://127.0.0.1:8549
# Output: 81984269328252633911 [8.198e19]

# sUSDe value in USDe
cast call 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 "convertToAssets(uint256)(uint256)" 81984269328252633911 --rpc-url http://127.0.0.1:8549
# Output: 99999999999999999999 [9.999e19]
```

---

## Test Artifacts

- **Strategy run log:** `/tmp/ethena_strategy_run.log`
- **Anvil log:** `/tmp/anvil_ethena.log`
- **Gateway log:** `/tmp/gateway_ethena.log`

---

## Conclusion

**PASS** - The ethena_yield strategy executed successfully on Anvil fork:
- Strategy logic correctly identified sufficient USDe balance
- STAKE intent properly compiled to 2 transactions
- Transactions executed successfully (130,105 gas)
- Position verified on-chain: 100 USDe staked → 82 sUSDe received
- ERC4626 vault conversion confirmed: sUSDe worth 100 USDe
- All components (gateway, compiler, adapter, runner) functioning correctly

**Key Success Factors:**
1. Proper Ethereum mainnet fork setup
2. Successful USDe token funding via whale transfer
3. Gateway connectivity to Anvil RPC
4. Ethena adapter correctly handling ERC4626 vault operations
5. Receipt parser correctly extracting staking events
6. Strategy state tracking working as expected

**No issues detected.**
