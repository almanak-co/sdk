# E2E Strategy Test Report: lido_staker

**Date:** 2026-02-08 18:17
**Result:** PASS
**Duration:** 7 minutes
**Worktree:** `<repo-worktree>/`

---

## Summary

lido_staker strategy executed successfully on Anvil Ethereum fork with config `receive_wrapped: false`. Strategy staked 4.95 ETH for stETH with 1 transaction (93,722 gas). No wstETH wrap attempted (as expected with `receive_wrapped: false`). Position verified on-chain.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | lido_staker |
| Chain | ethereum |
| Network | Anvil fork |
| Port | 8549 |
| Min Stake Amount | 0.1 ETH |
| Gas Reserve | 0.05 ETH |
| Receive Wrapped | false |
| Initial Funding | 5.0 ETH |

---

## Lifecycle Phases

### Phase 1: Setup
- [x] Anvil started on port 8549 (Ethereum mainnet fork)
- [x] Gateway started on port 50051
- [x] Wallet funded: 5.0 ETH

### Phase 2: Strategy Execution
- [x] Strategy ran successfully
- [x] Intent executed: STAKE (4.95 ETH -> stETH)
- [x] Single transaction, no wrap operation

### Phase 3: Verification
- [x] stETH balance: 4.95 ETH (4,949,999,999,999,999,998 wei)
- [x] Remaining ETH: 0.0499 ETH (gas reserve + transaction costs)
- [x] Transaction gas used: 93,722

---

## Execution Log Highlights

### Strategy Run
```text
Using config: strategies/demo/lido_staker/config.yaml
Connected to gateway at localhost:50051

Strategy: LidoStakerStrategy
Instance ID: LidoStakerStrategy:3178ca8ed0d1
Chain: ethereum
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

ETH balance (5) - gas_reserve (0.05) = 4.95 >= min_stake (0.1), staking
STAKE intent: 4.9500 ETH -> stETH

Compiled STAKE intent: 4.95 ETH via lido, 1 txs, 100000 gas
Execution successful: gas_used=93722, tx_count=1
Parsed Lido receipt: stakes=1, wraps=0, unwraps=0
Staking successful: 4.95 ETH -> stETH

Status: SUCCESS | Intent: STAKE | Gas used: 93722 | Duration: 5867ms
```

---

## Transactions

| Phase | Intent | Gas Used | Status |
|-------|--------|----------|--------|
| Execute | STAKE | 93,722 | ✅ |

---

## Final Verification

### stETH Balance
```text
Contract: 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
Balance: 4949999999999999998 wei (4.95 stETH)
```

### ETH Balance
```text
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
Balance: 0.049901987058850214 ETH (gas reserve + transaction costs)
```

### Gas Breakdown
- **Initial ETH**: 5.0 ETH
- **Staked**: 4.95 ETH
- **Gas Used**: 0.000098012941149786 ETH (~93,722 gas)
- **Remaining**: 0.049901987058850214 ETH

---

## Strategy Configuration Details

### Config Values
```yaml
min_stake_amount: "0.1"
gas_reserve: "0.05"
receive_wrapped: false
force_action: ""
```

### Decision Logic
```python
# Strategy calculated:
# ETH balance (5.0) - gas_reserve (0.05) = 4.95
# 4.95 >= min_stake (0.1) => STAKE 4.95 ETH
# receive_wrapped: false => Direct stETH receipt (no wrap to wstETH)
```

---

## Key Observations

1. **Config Change Validated**: Strategy respected `receive_wrapped: false` and only executed STAKE action (no wrap to wstETH).

2. **Gas Efficiency**: Single transaction at 93,722 gas (~$2-3 at typical mainnet prices), which is optimal for simple staking.

3. **Balance Accuracy**: stETH balance (4.95 ETH) exactly matches staked amount minus 2 wei (rounding), confirming correct execution.

4. **Gas Reserve Respected**: Strategy left 0.05 ETH untouched as gas_reserve, preventing account from being drained.

5. **No Wrap Attempt**: As expected from context, strategy did NOT attempt wstETH wrap (which would revert on Anvil due to Lido rebasing stETH issue).

---

## Conclusion

**PASS** - Strategy executed successfully in single-transaction mode with `receive_wrapped: false`. Staked 4.95 ETH for stETH, verified on-chain balance, no errors or reverts. Gas reserve respected, configuration correctly applied.

---

## Test Environment

- **Anvil**: Ethereum mainnet fork (chain ID: 1, port 8549)
- **Gateway**: localhost:50051 (anvil network mode)
- **Worktree**: `<repo-worktree>/`
- **Test Wallet**: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default)
- **Alchemy RPC**: Used for mainnet fork via `.env` file
