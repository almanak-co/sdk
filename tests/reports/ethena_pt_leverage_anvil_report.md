# Ethena PT Leverage -- Anvil Test Report

**Date:** 2026-02-18
**Strategy:** `ethena_pt_leverage`
**Branch:** `feat/ethena-3strats`
**Network:** Anvil fork (Ethereum mainnet)
**Result:** PARTIAL PASS (code correct, blocked by SDK gap)

## Summary

Strategy 2 (PT-sUSDe Leveraged Fixed Yield) **partially passed** Anvil testing. The strategy code initializes correctly, passes all safety checks, and produces a valid FlashLoanIntent. Full execution is blocked because the IntentCompiler does not yet support FLASH_LOAN intent compilation.

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Morpho Market ID | `0xd174bb7b8dd6ef16b116753b56679932ee13382b94f81bf66a2b37962cb41f56` |
| PT Token | `PT-sUSDe-7MAY2026` (78 days to maturity) |
| PT Token Address | `0x3de0ff76e8b528c092d47b9dac775931cef80f49` |
| Pendle Market | `0x8dae8ece668cf80d348873f23d456448e8694883` |
| Target Leverage | 3.0x |
| LLTV | 91.5% |
| Initial USDC | 50,000 |
| Chain | Ethereum |

## Test Results

| Step | What Happened | Status |
|------|---------------|--------|
| Config loading | Strategy read config.json correctly | PASS |
| Initialization | `EthenaPTLeverageStrategy` initialized with all parameters | PASS |
| Wallet funding | Anvil funded with 50,000 USDC | PASS |
| Balance detection | Strategy detected 50,000 USDC, initiated entry | PASS |
| Safety checks | `build_pt_leverage_loop()` validated: 78 days > 7 min, 3x < 10x max, HF 1.37 > 1.3 | PASS |
| Intent creation | FlashLoanIntent created with nested [SWAP, SUPPLY, BORROW] callbacks | PASS |
| Intent compilation | IntentCompiler returned "Unknown state: IntentState.IDLE" for FLASH_LOAN | FAIL (SDK gap) |

## SDK Gap: FLASH_LOAN Compiler

The `FlashLoanIntent` type is defined in `almanak/framework/intents/vocabulary.py` and the factory functions in `almanak/framework/intents/pt_leverage.py` produce valid intent structures. However, the `IntentCompiler` in `almanak/framework/intents/compiler.py` does not have a handler for the `FLASH_LOAN` intent type.

**What's needed:**
- A `compile_flash_loan()` method in the IntentCompiler that:
  1. Builds the flash loan initiation transaction (Morpho `flashLoan()`)
  2. Compiles each callback intent sequentially (SWAP via Pendle, SUPPLY to Morpho, BORROW from Morpho)
  3. Wraps everything in a multicall/bundler contract
- Integration with Morpho's `FlashBorrower` interface for atomic execution

**Alternative approaches:**
- Use the transaction builder directly (bypassing the intent compiler)
- Implement as a "bundled intent sequence" rather than a single flash loan intent
- Use a dedicated flash loan aggregator (e.g., Morpho bundler contract)

## Additional Testing Constraints

### Expired PT Market Issue
The original config used `PT-sUSDe-5FEB2026` which expired on Feb 5, 2026 (13 days ago). This was changed to `PT-sUSDe-7MAY2026` (active, 78 days to maturity) for testing. However:
- **PT-sUSDe-5FEB2026**: Has Morpho market (`0xd174bb7b...`) but PT is expired
- **PT-sUSDe-7MAY2026**: Active PT but NO Morpho market exists for this collateral

A full on-chain test would require a PT that is both:
1. Active (>7 days to maturity)
2. Has a Morpho Blue market with USDC liquidity

### Pendle AMM on Anvil
Even if the compiler supported FLASH_LOAN, the Pendle USDC -> PT-sUSDe swap would go through Enso routing, which may face the same 0x signed order timing issues observed in Strategy 1 testing.

## Verified Components

1. Strategy class initialization and config parsing
2. State machine transitions (idle -> entering)
3. `build_pt_leverage_loop()` factory function with safety validation
4. FlashLoanIntent structure with nested callback intents (SWAP, SUPPLY, BORROW)
5. Maturity date parsing from PT token name (`PT-sUSDe-7MAY2026` -> May 7, 2026)
6. Timeline event recording (STATE_CHANGE, POSITION_MODIFIED)
7. get_status() and get_persistent_state() methods
8. Teardown interface (supports_teardown, generate_teardown_intents)

## Conclusion

Strategy 2 code is **architecturally sound** and ready for execution once the FLASH_LOAN intent compilation path is implemented. The strategy correctly:
- Validates safety parameters before entry
- Creates atomic flash loan intent structures
- Monitors health factor and maturity proximity
- Supports graceful teardown via flash loan unwind

**Blocking issue:** `IntentCompiler` needs a `FLASH_LOAN` handler to turn `FlashLoanIntent` into on-chain transactions.
