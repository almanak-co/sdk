---
name: codebase-patterns
description: Patterns confirmed across 2+ Kitchen Loop iterations about how the almanak-sdk codebase works
type: project
---

# Codebase Patterns (confirmed across 2+ iterations)

## Connector Patterns

- **Standard structure**: Every protocol connector follows `connectors/{protocol}/sdk.py`, `adapter.py`, `receipt_parser.py` pattern. Confirmed across Uniswap V3, V4, Aerodrome, PancakeSwap, TraderJoe, Aave, Morpho, GMX, Curve, Enso, BENQI, Pendle (iters 40-85).
- **Receipt parser extraction methods**: Parsers must expose `extract_position_id()`, `extract_swap_amounts()`, etc. for the `ResultEnricher` to call automatically. Missing these = broken strategy author UX (confirmed iters 71, 72, 82).
- **Token resolution fail-fast**: Never default to 18 decimals. `TokenNotFoundError` is correct behavior. Silent fallback caused real bugs (iter 85: #731 removed Uniswap V4 silent 18-decimal fallback).
- **Derivative token min_amount_out**: 1:1 price estimates break for derivative tokens that trade at discount/premium (PT tokens, yield-bearing wrappers). Confirmed iters 39, 88, 89: Pendle PT-wstETH trades at ~80.7% of underlying. Fix: use protocol-specific quoters or discounted estimates (#750).
- **amount='all' resolution gap**: The strategy runner resolves `amount='all'` for multi-intent sequences and teardown paths, but NOT for single intents from `decide()`. This is a recurring surprise for strategy authors. Confirmed iters 88 (pendle_pt_sell_validator BUG-1, VIB-1423). Fix in PR #779.
- **Connector portability is strong**: Well-established connectors (Uniswap V3, Aave V3) work on new chains with zero code changes. Confirmed: UniV3 on 7 chains (arbitrum, base, optimism, ethereum, avalanche, polygon, mantle), Aave V3 on 8 chains (+ Sonic in iter 100), Compound V3 on 4 chains (+ Optimism in iter 101). The blockers are always config entries (PROTOCOL_ROUTERS, SWAP_QUOTER_ADDRESSES, COMET_ADDRESSES, token registry addresses), not connector logic. Confirmed iters 49, 51, 90, 94, 100, 101.
- **VIB-592 swap_amounts decimals bug is cross-chain**: Receipt parser divides both token amounts by 10^18 regardless of actual token decimals (e.g., USDC=6). Confirmed on Arbitrum (iter 44), Ethereum (iter 61), Polygon (iter 94). Affects all Uniswap V3 swaps where token_in != 18 decimals.

## Connector Patterns (continued)

- **Curve adapter gas estimates are chronically low**: `exchange()` and `add_liquidity()` calls routinely underestimate gas. Confirmed in iters 95 (CryptoSwap + Aave pipeline) and 97 (3pool LP on Ethereum). Pattern: any iteration that adds a new Curve operation should bump the gas estimate constant. Check `connectors/curve/adapter.py` gas estimate constants when debugging Curve simulation failures.
- **Template scaffolding can silently break**: `almanak strat new` templates generate runnable-looking code that actually fails immediately (broken imports, placeholder addresses failing Pydantic, incorrect intent maps). This is not caught by the scaffold unit tests which only check file structure, not runtime behavior. Confirmed iter-98 audit: 3/10 templates had P0 runtime failures.

## Token & Chain Patterns

- **Non-standard ERC-20 storage slots block Anvil wallet funding**: The `fork_manager.py` brute-force storage slot probe fails for tokens with non-standard storage layouts (e.g., WETH on Sonic). When funding fails, test wallets start with 0 of that token. USDC tends to use standard slot 9 across chains. WETH varies. Confirmed iter 100: USDC on Sonic = standard (slot 9), WETH on Sonic = non-standard (brute-force fails). Impact: lifecycle tests that require REPAY fail because borrowed amount doesn't include accrued interest.
- **Stale .pyc cache hides token registry updates**: After editing `defaults.py` or `fork_manager.py` to add token addresses, stale Python bytecode cache (`.pyc`) may prevent changes from taking effect. The token resolver silently fails to find newly added tokens. Fix: delete the relevant `.pyc` file or run `find . -name "*.pyc" -delete`. Confirmed iter 100: resolver missed Sonic WETH/USDC until pyc cleared.

## Testing Patterns

- **Demo strategy quick regression**: 4 chains (arbitrum, base, avalanche, optimism), one strategy each. Stable test set: uniswap_rsi, aerodrome_lp, traderjoe_lp, uniswap_lp. Consistent across iters 80-99. Note: `make test-demo-quick` target doesn't pass `--quick` flag -- must run script directly with `--quick`. aerodrome_lp/base consistently FAILs (pre-existing since iter 90: strategy spawns own mainnet gateway).
- **uniswap_rsi timing**: Highly variable (4s to 110s) depending on gateway startup state. The 120s demo timeout is a real risk -- 72.3s seen in iter 99. RPC latency and flash loan pre-warm are the main factors.
- **Anvil time-warp for time-locked DeFi mechanics**: `evm_increaseTime(seconds)` + `evm_mine` integrates cleanly with the framework on Anvil forks. The warped timestamp is respected by `eth_estimateGas`, so simulations pass after advancing time. Confirmed iter 99 (Ethena 7-day cooldown, VIB-1496). Mark as GATEWAY_VIOLATION -- these Anvil-only methods have no mainnet equivalent and no gateway path. Use for: cooldowns, vesting schedules, expiry logic.
- **Function selectors must be verified with cast sig**: Never trust documentation or code comments for function selectors. Verify with `cast sig "functionName(type)"`. Confirmed iter 99: `ETHENA_UNSTAKE_SELECTOR` was wrong (`unstake(uint256)` vs `unstake(address)`) -- a silent bug that only manifested when the complete_unstake path was first exercised.
- **Intent test 4-layer verification**: Compilation -> Execution -> Receipt Parsing -> Balance Deltas. Tests missing balance deltas have missed real bugs (confirmed multiple iterations).

## Error Patterns

- **V3 DEX LP_CLOSE simulation revert**: All V3-style NFT position DEXes (Uniswap V3, PancakeSwap V3, SushiSwap V3) fail on decreaseLiquidity simulation. Confirmed across iters 40, 46, 71, 72. Only non-NFT gauge mechanisms (Aerodrome/Velodrome) work.
- **Gateway startup race**: Gateway gRPC calls can fail if strategy starts before gateway is ready. Fixed by wait_for_ready retry loop (#717, iter 84). Pattern: always use retry/backoff for gateway connections.

## Performance Patterns

- **Anvil fork block freshness**: Fork blocks should be recent (within 7 days) to avoid stale contract state. Old blocks cause false test failures.
- **Demo test throughput**: Quick regression takes ~70s total (4 chains sequential). Full regression (21+ strategies) takes ~15-30 min depending on RPC.

## Merge Velocity

- **Peak**: 20 PRs merged across iters 85-89, 16 in single iter 85, 14 in iter 82. The loop sustains high merge rates without regressions when changes are well-scoped.
- **Long-lived branches**: Dangerous for config files. Merge conflicts silently revert merged work (iter 82: #669 reverted #658's REGRESS_QUICK default).
