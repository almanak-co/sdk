# E2E Strategy Test Report: rsi_martingale_short (Anvil)

**Date:** 2026-02-20 09:05
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | rsi_martingale_short |
| Chain | arbitrum |
| Network | Anvil fork (managed, auto-started on random port) |
| Protocol | GMX V2 (perpetual futures) |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `risk_budget_usd` | `"100"` | `"50"` | Budget cap: $50 max per trade |
| `force_action` | `null` | `"open"` | Force immediate PERP_OPEN for testing |

Both changes were **restored after the test** (config.json is back to its original state).

Note: The initial collateral computed by the martingale formula at $50 budget, 5 max_doublings is:
`$50 / (2^6 - 1) = $50 / 63 = $0.79 USDC` - well under the $50 cap per trade.

## Execution

### Setup
- [x] Anvil started on port 8545 (manual, unused - see note)
- [x] Managed gateway auto-started by CLI on 127.0.0.1:50052
- [x] CLI also auto-started its own Anvil fork on port 55254 (block 434037583, chain_id=42161)
- [ ] Wallet funding: NOT applied to the managed fork (manual funding targeted port 8545, not 55254)

**Note on Anvil fork:** `uv run almanak strat run --network anvil` starts a fully managed gateway
that creates its own fresh Anvil fork on a random port. This fork starts with zero token balances.
Manual pre-funding of a separate Anvil instance on port 8545 had no effect on the strategy's fork.

### Strategy Run
- [x] Strategy loaded successfully: `RSIMartingaleShortStrategy`
- [x] Force action triggered: `open`
- [x] Intent decided: `PERP_OPEN - SHORT ETH/USD, $1.58 size (2.0x leverage, $0.79 USDC collateral)`
- [x] ETH price fetched from CoinGecko: $1,966.68
- [x] GMX V2 MARKET_INCREASE order compiled: 1 transaction, 3,900,000 gas
- [x] Transaction submitted (4 times: 1 attempt + 3 retries)
- [x] All 4 transactions reverted

### Transaction Hashes (all reverted)

| Attempt | TX Hash |
|---------|---------|
| 1 | `70120c9e1ed6a2447152c1fb95833a33836752f0767c8f2916a22d80cddf1550` |
| 2 | `881161ff6bb79139efebcc07cc42061438d8150d5d9eeafffd3d52bca7a35d9f` |
| 3 | `e58f35e21c97589718342561e0788b021f80121eef5d2e26f6eb19b4b6af812a` |
| 4 | `2513ab8f6ef65e5503dfbd465a51b5f58df2e09984cba975732b385c210fa6bf` |

(All on the managed Anvil fork, not a public network - no explorer links applicable)

## Error Details

```text
Transaction reverted: Error: ERC20: transfer amount exceeds allowance

REVERT DIAGNOSTIC:
  Intent: PERP_OPEN
  Chain: arbitrum
  Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

  Native ETH (gas + execution fees):
    ✓ Native ETH: 9999.999429 (need 0.001500)

  Token Balances vs Requirements:
    ✗ USDC: 0.000000 (need 0.790000, short 0.790000)

  Likely Cause: Insufficient balance for: USDC
  Suggestion: Acquire 0.790000 USDC via swap or bridge
```

## Root Cause Analysis

The GMX V2 adapter does not include an `approve()` transaction before the `createOrder()` call.
When the Anvil fork starts fresh, the wallet has zero USDC and zero USDC allowance for the
GMX V2 OrderVault contract. The ERC20 transfer reverts with `transfer amount exceeds allowance`.

Two related issues:

1. **Missing auto-approval step in GMX V2 adapter**: The `PERP_OPEN` compiled bundle only
   contains 1 transaction (the `createOrder()` call). It should prepend a `USDC.approve()`
   transaction to the GMX V2 OrderVault (`0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5`)
   before the order creation. Other protocol adapters (e.g., Uniswap V3, Aave) handle
   approvals automatically.

2. **No `anvil_funding` config support**: The managed Anvil gateway does not fund the wallet
   with test tokens. Adding an `anvil_funding` block to `config.json` would be the correct
   solution for test reproducibility (if the framework supports it for incubating strategies).

### Key Log Output

```text
INFO  RSIMartingaleShort initialized: market=ETH/USD, budget=$50.00,
      initial_collateral=$0.79, leverage=2.0x, max_doublings=5, RSI>75, retracement_target=30%

INFO  Force action: open
INFO  SHORT L0: $0.79 USDC collateral -> $1.58 position @ 2.0x | entry price=$1,966.68
INFO  PERP_OPEN intent: SHORT ETH/USD $1.58 (2.0x) via gmx_v2
INFO  GMX V2 order requires ~0.0020 native token as keeper execution fee
INFO  Compiled PERP_OPEN intent: SHORT ETH/USD, $1.580 size, 1 txs, 3900000 gas
ERROR FAILED: PERP_OPEN - Transaction reverted at 70120c...1550
ERROR    Reason: Error: ERC20: transfer amount exceeds allowance
ERROR Intent failed after 3 retries
```

## Strategy Behavior Assessment

The strategy logic itself is functioning correctly:

- Loaded and initialized cleanly
- Parsed config correctly (budget=$50, initial_collateral=$0.79)
- `force_action: "open"` bypassed RSI/rally filters as expected
- Fetched live ETH price ($1,966.68) successfully
- Constructed a valid GMX V2 MARKET_INCREASE order
- The `decide()` method returned the correct `PERP_OPEN` intent

The failure is in the **execution layer** (missing USDC approval in GMX V2 adapter),
not in the strategy logic itself.

## Result

**FAIL** - GMX V2 PERP_OPEN reverted with `ERC20: transfer amount exceeds allowance` on all
4 attempts (1 + 3 retries). The managed Anvil fork starts with zero USDC balance and the
GMX V2 adapter does not include an auto-approval step for the collateral token before
submitting the `createOrder()` transaction.

## Recommended Fix

The GMX V2 adapter's `PERP_OPEN` compilation should prepend a `USDC.approve(OrderVault, amount)`
action to the `ActionBundle` when the collateral token is an ERC-20. This is consistent with how
other protocol adapters handle token approvals automatically.
