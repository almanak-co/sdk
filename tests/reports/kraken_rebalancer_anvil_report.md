# Anvil Test Report: kraken_rebalancer

**Date:** 2026-02-08 15:09
**Result:** FAIL
**Duration:** 2 minutes

---

## Summary

The `kraken_rebalancer` strategy failed to initialize due to a code error: "property 'chain' of 'KrakenRebalancerStrategy' object has no setter". Additionally, this is a CEX (centralized exchange) strategy that requires Kraken API credentials which are not configured in the environment.

---

## Configuration Analysis

| Field | Value | Notes |
|-------|-------|-------|
| Strategy | kraken_rebalancer | CEX integration demo |
| Config File | **MISSING** | No config.json found in strategy directory |
| Chain | arbitrum | Default chain from strategy code |
| Network | Anvil fork | Port 8545 |
| Kraken API | **NOT CONFIGURED** | KRAKEN_API_KEY and KRAKEN_API_SECRET missing from .env |

---

## Prerequisites Status

### Required Files
- [ ] `config.json` - **MISSING** (strategy has only strategy.py)
- [x] `strategy.py` - Present
- [x] `__init__.py` - Present

### Environment Variables
- [x] `ALMANAK_PRIVATE_KEY` - Configured (Anvil default)
- [x] `ALCHEMY_API_KEY` - Configured
- [ ] `KRAKEN_API_KEY` - **MISSING** (lines 55-56 in .env are commented out)
- [ ] `KRAKEN_API_SECRET` - **MISSING** (lines 55-56 in .env are commented out)

---

## Test Execution Log

### Phase 1: Setup
- [x] Anvil started on port 8545 (Arbitrum fork, chain ID 42161)
- [x] Gateway started on port 50051
- [ ] Wallet funded - **SKIPPED** (strategy failed before funding)

### Phase 2: Strategy Execution
```
Connecting to gateway at localhost:50051...
Connected to gateway at localhost:50051

Loaded strategy: KrakenRebalancerStrategy
============================================================
ALMANAK STRATEGY RUNNER
============================================================
Strategy: KrakenRebalancerStrategy
Instance ID: KrakenRebalancerStrategy:3fb745897d88
Mode: FRESH START (no existing state)
Chain: arbitrum
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
Execution: Single run
Dry run: False
Gateway: localhost:50051
============================================================
Strategy class loaded: KrakenRebalancerStrategy
  Config wrapped in DictConfigWrapper

Error creating strategy instance: property 'chain' of 'KrakenRebalancerStrategy' object has no setter
```

---

## Error Analysis

### Primary Error: Property Setter Missing
```
Error creating strategy instance: property 'chain' of 'KrakenRebalancerStrategy' object has no setter
```

**Root Cause:** The strategy initialization code attempts to set the `chain` property, but the `KrakenRebalancerStrategy` class doesn't have a setter for this property. This is a code bug in the strategy implementation.

**Location:** The error occurs during strategy instantiation in the runner framework when it tries to set the chain from the config/environment.

### Secondary Issue: Missing Kraken API Credentials

Even if the property setter issue were fixed, the strategy would run in **simulation mode** because:

1. Lines 55-56 in `.env` are commented out:
   ```bash
   # KRAKEN_API_KEY=
   # KRAKEN_API_SECRET=
   ```

2. Strategy code (lines 207-220 in strategy.py) handles this gracefully:
   ```python
   try:
       self.credentials = KrakenCredentials.from_env()
       self.kraken_config = KrakenConfig(credentials=self.credentials)
       self.sdk = KrakenSDK(credentials=self.credentials)
       self.adapter = KrakenAdapter(config=self.kraken_config, sdk=self.sdk)
       self._kraken_available = True
       logger.info("Kraken SDK initialized successfully")
   except Exception as e:
       logger.warning(f"Kraken SDK initialization failed: {e}")
       logger.warning("Strategy will run in simulation mode")
       self._kraken_available = False
   ```

3. In simulation mode, the strategy logs actions but doesn't execute real trades:
   - Deposit: `[SIMULATION] Would deposit to Kraken`
   - Swap: `[SIMULATION] Would execute swap on Kraken`
   - Withdraw: `[SIMULATION] Would withdraw from Kraken`

---

## Strategy Design Notes

This is a **CEX integration strategy**, not a traditional on-chain DeFi strategy. It demonstrates:

1. **Deposit**: Transfer USDC from on-chain wallet to Kraken
2. **Swap**: Execute USDC → ETH trade on Kraken's orderbook
3. **Withdraw**: Transfer ETH from Kraken back to on-chain wallet

**Why it differs from other demo strategies:**
- Requires external API credentials (Kraken)
- Core trading logic happens off-chain on Kraken's CEX
- On-chain interactions are only for deposit/withdrawal
- Cannot be fully tested on Anvil without real Kraken API access

---

## Missing Config File

Unlike other demo strategies, `kraken_rebalancer` has **no config.json** file:

```bash
$ ls -la strategies/demo/kraken_rebalancer/
total 72
-rw-r--r--@  1 nick  staff    204 Jan 26 22:52 __init__.py
drwxr-xr-x@  4 nick  staff    128 Feb  3 15:27 __pycache__
-rw-r--r--   1 nick  staff  30042 Jan 31 09:25 strategy.py
```

The strategy's documentation (lines 166-174) suggests a config structure:
```json
{
    "swap_amount_usd": 10,
    "chain": "arbitrum",
    "from_token": "USDC",
    "to_token": "ETH",
    "skip_deposit": false,
    "skip_withdraw": false
}
```

But this file doesn't exist in the repository.

---

## Recommendations

### To Fix This Strategy

1. **Fix the property setter error:**
   - Investigate why the `chain` property lacks a setter
   - Check if the IntentStrategy base class properly defines the chain property
   - Add a setter or adjust how the runner initializes the strategy

2. **Add config.json:**
   - Create `strategies/demo/kraken_rebalancer/config.json` with the documented schema
   - Include sensible defaults (chain=arbitrum, swap_amount_usd=10)

3. **Document simulation mode:**
   - Add clear documentation that this strategy runs in simulation mode without API credentials
   - Show example output of simulation mode in the README

### For Testing

This strategy **cannot be meaningfully tested on Anvil** without Kraken API credentials because:
- The core logic (swap on CEX) happens off-chain
- Simulation mode just logs placeholder messages
- No on-chain transactions are actually executed (deposit/withdraw are skipped in simulation)

**To properly test:**
1. Set up a Kraken account
2. Generate API keys with appropriate permissions
3. Configure `KRAKEN_API_KEY` and `KRAKEN_API_SECRET` in `.env`
4. Whitelist the wallet address on Kraken (24-72 hour verification)
5. Run on mainnet or testnet with actual CEX connectivity

---

## Conclusion

**FAIL** - Strategy initialization failed with code error: "property 'chain' of 'KrakenRebalancerStrategy' object has no setter".

**Secondary Issues:**
- Missing `config.json` file
- Missing Kraken API credentials (expected for this CEX strategy)
- Cannot be properly tested on Anvil without external API access

**Classification:** Code bug preventing strategy instantiation. This strategy requires code fixes before it can run, even in simulation mode.
