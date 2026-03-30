# Changelog

All notable changes to the Almanak SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2.7.0] - 2026-03-30

### Added
- Uniswap V4 full support: Phase 0 contract verification (#1096), Phase 1 UniversalRouter + Permit2 swap (#1098), Phase 2 PositionManager LP adapter + HookFlags (#1100), Phase 3 hook discovery + hookData encoding (#1119), demo strategies (#1120, #1139), 4-layer intent tests for swap (#1138) and LP lifecycle (#1146)
- Framework-owned portfolio valuation engine with protocol-specific valuers (#1103)
- Position discovery service for on-chain position detection (#1127)
- LP position re-pricing via V3 math (#1109)
- Lending position re-pricing via Aave V3 on-chain data (#1115)
- GMX V2 perps valuer for mark-to-market position pricing (#1142)
- Paper trading valuation alignment with PortfolioValuer integration (#1137)
- Paper trading batch 2: callback parity, resume CLI, force_action guard (#1084)
- Paper trading batch 3: indicator fallback, fork RPC, health telemetry (#1091)
- GMX V2 REST API fallback for position queries (#1086)
- TraderJoe V2 swap via LBRouter2 for BTC.b routing on Avalanche (#1106)
- Expose public price_to_tick/tick_to_price utilities (#1124)
- L3 semantic verification for swap intent tests (#1159)
- Reference strategies: top 3 curated DeFi examples (#1077)
- New demo strategies: Morpho Blue paper trade (#1141), Compound V3 PnL backtest on Polygon (#1148), PancakeSwap V3 RSI parameter sweep on BSC (#1165), Velodrome V2 swap on Optimism (#1150), TraderJoe V2 LP on Avalanche (#1151), TraderJoe leveraged LP with auto-compound (#1147), PancakeSwap V3 paper trade on BSC (#1082), Uniswap V3 swap on BSC (#1085), PancakeSwap V3 swap on Ethereum (#1095), Compound V3 + Uniswap V3 leveraged yield on Arbitrum (#1104), Aave V3 + Velodrome V2 leveraged LP on Optimism (#1163), Aave V3 + Enso leveraged swap on Sonic (#1179)
- Crisis scenario backtests: Aerodrome swap on Base (#1121), TraderJoe V2 LP on Avalanche (#1140)
- Curve CryptoSwap 4-layer intent test on Ethereum (#1105)
- Comprehensive V4 ACTION_* byte validation + calldata encoding tests (#1168)
- 27 unit tests for Compound V3 PnL Polygon strategy (#1166)

### Fixed
- V4 swap two-layer encoding + correct action bytes (#1160)
- V4 receipt parser + WETH routing via native ETH pools (#1167)
- V4 receipt parser Transfer amount-fallback for enrichment (#1131)
- V4 LP_CLOSE on-chain liquidity query (#1174)
- Correct Uniswap V4 per-chain addresses from official docs (#1156)
- Remove stale V4 swap xfail markers + add keccak topic verification (#1164)
- Deduplicate HookFlags with single source of truth (#1130)
- Paper trading event loop crash and RSI type mismatch (#1145)
- Paper trading batch 1: resume URL, hex crash, port contention (#1081)
- Merge strategy state in runner to prevent position_id loss (#1113)
- GMX V2 PERP_CLOSE reads on-chain size to stop burning keeper fees (#1094)
- Aerodrome enrichment swap_amounts fallback + lower log level (#1090)
- AerodromeSDK checksum failure causes zero slippage protection (#1089)
- Respect strategy teardown slippage in escalation manager (#1088)
- LP_CLOSE blocked by price gate when no tokens extractable (#1078)
- Query on-chain balance in get_open_positions() for swap strategies (#1080)
- Route Enso API calls through gateway gRPC in deployed mode (#1102)
- Swap ETH/WETH clarity + suppress batch token resolution warnings (#1101)
- LocalSimulator state-setup TXs hang on EIP-1559 chains (#1097)
- Anvil --no-gas-cap crash and strategy init kwargs TypeError (#1092)
- Anvil mode always uses 9999 gwei gas cap (#1154)
- Bound Anvil --no-gas-cap version fallback to 0.x series (#1122)
- Metrics server port conflict with graceful fallback to ephemeral port (#1133)
- Correct misleading Fluid swap min-amount error (#1093)
- Decode Fluid DEX pool capacity errors with actionable messages (#1117)
- Persist portfolio snapshots when using gateway state manager (#1108)
- Derive Compound V3 support_matrix chains from COMET_ADDRESSES (#1123)
- Add WETH.e (slot 0) to KNOWN_BALANCE_SLOTS for Avalanche (#1125)
- Add ATH and TORIVA to default token registry (#1110)
- Enforce bilateral balance deltas in all swap intent tests (#1158)
- Dashboard explorer links, duplicate instances, empty protocol (#1175)
- Publish gateway image to both registries on prod release (#1099)
- Skip Release workflow for pre-releases (RC tags) (#1107)
- gasPrice fallback and strategy retry from loop review (#1114)

## [2.6.4] - 2026-03-26

### Added
- Compound V3 WETH market lifecycle on Arbitrum (#1016)
- PostgreSQL backend for TimelineStore for deployed dashboards (#1022)
- Fluid DEX swap intent test with 4-layer verification (#1013)
- BENQI full lending lifecycle on Avalanche (#1049)
- Curve 3pool paper trade strategy on Ethereum (#1057)
- TraderJoe V2 LP bin-width sweep on Avalanche (#1015)
- Aerodrome LP range_width_pct parameter sweep on Base (#1043)
- Crisis scenario backtest for Uniswap V3 swap on Arbitrum (#1042)
- PancakeSwap V3 PnL backtest swap strategy on BSC (#1053)
- Aave V3 lending parameter sweep on Arbitrum (#1052)
- Aave V3 paper trade leverage loop on Polygon (#1069)
- Aave V3 paper trade lending strategy on Polygon (#1074)
- Uniswap V3 RSI PnL backtest on Arbitrum (#1075)
- Uniswap V3 RSI parameter sweep on Arbitrum (#1073)
- SushiSwap V3 PnL backtest on Base (#1072)
- Aerodrome SWAP + Compound V3 lending lifecycle on Base (#1071)
- Compound V3 PnL backtest on Base (#1068)
- SDK marketing video composition (#1076)
- Bump almanak-code to v0.2.8

### Changed
- Feature strategy params in optimize/walk-forward CLI help text (#1064)

### Fixed
- GMX V2 Reader position queries with fallback mechanisms (#1067)
- Bridge gas estimation: raise default and enable eth_estimateGas for all TXs (#1063)
- GMX V2 close_position uses sentinel value for full close without cache (#1062)
- Set SUPPORTED_CHAINS in decorator for multi-chain detection (#1040)
- Sum Alchemy sub-call gas per transaction instead of flattening (#1061)
- Balancer flash_arb EOA fallback to swap mode (#1056)
- Increase pendle_yt_yield teardown slippage for illiquid YT (#1055)
- Aave borrow teardown withdraw amount resolution (#1054)
- Suppress unclosed aiohttp ClientSession warning on gateway shutdown (#1047)
- Increase LocalSimulator state setup timeout from 10s to 30s (#1046)
- Enable Velodrome/Aerodrome swap compilation on Optimism (#1045)
- Resolve gateway API keys from ALMANAK_GATEWAY_ prefixed env vars (#1041)
- Reject deprecated stable interest rate mode for Aave V3 and Spark (#1033)
- Decode Fluid DEX revert errors and lower demo trade size (#1032)
- Add on_intent_executed/save_state callbacks to bridge-waiting path (#1031)
- Use bare API key env var names, add settings fallbacks (#1037)
- Restore almanak.wallets entry point for sidecar wallet resolution (#1035)
- Curve LP position_id as address with on-chain balance query (#1030)
- Initialize EXECUTE_SKIPPED_BACKPRESSURE to prevent unbound variable crash (#1029)
- Register Curve NG pool LP tokens in token resolver (#1025)
- Curve NG 4-coin pool receipt parsing: include all coin amounts (#1024)
- Address 5 chronic Kitchen Loop issues from meta-analysis (#1023)
- Strip pool-type suffixes from token extraction (#1000)
- Curve LP extract_liquidity() returns human-readable Decimal (#999)
- Resolve swap amount decimals from Transfer events instead of defaulting to 18 (#1009)
- Case-insensitive collateral lookup in Compound V3 adapter (#1017)
- Increase Anvil funding for 2 failing strategies (#1012)
- Allow single-chain sidecar mode with ALMANAK_GATEWAY_WALLETS (#1010)
- Resolve empty wallet_address in multi-chain sidecar mode (#1006)
- Anvil version detection to skip --no-gas-cap on 0.3.x (#996)
- Cache parse_receipt in ResultEnricher (#989)
- Paper trading balance cache and RSI return type (#965)
- Align Curve LP intent tests with human-readable extract_liquidity() (#1026)
- Normalize AGENT_ID across all deployed dashboard data paths (#1028)
- Isolate market service test from CI env vars (#1038)
- Use COVERAGE_CORE=sysmon for near-zero coverage overhead (#1005)

## [2.6.3] - 2026-03-23

### Added
- Multi-chain sidecar deployment mode: strategies can run with `--no-gateway` using per-chain wallet config via `ALMANAK_GATEWAY_WALLETS` (#1003)

### Fixed
- Swap amounts decimals resolved from Transfer events instead of defaulting to 18, fixing wrong values for non-18-decimal tokens like USDC (#997)
- CLI native fallback command now forwards `--flags` correctly to almanak-code binary

## [2.6.2] - 2026-03-23

### Added
- Forward unknown CLI args to native almanak-code binary via execv, enabling `almanak acp --dangerously-skip-permissions` passthrough

### Fixed
- Fix CLI test crashes from `ignore_unknown_options` swallowing subcommand flags
- Sync all version files (_version.py, SKILL.md) to match pyproject.toml
- Restore MONAD chain and indicator methods (adx, obv, cci, ichimoku) in strategy-builder skill

## [2.6.1] - 2026-03-23 [YANKED]

### Added
- Forward unknown CLI args to native almanak-code binary via execv, enabling `almanak acp --dangerously-skip-permissions` passthrough

### Fixed
- Docs sync: add Agni/Fluid connectors, update strategy-builder skill version

## [2.6.0] - 2026-03-23

### Added
- Gateway multi-wallet abstraction: per-chain wallet config with cross-chain IntentSequence execution (#970)
- Price impact guard in compiler: fails swap when quoter deviates >50% from oracle (#988)
- Fluid DEX connector for Arbitrum with swap support (#904)
- LinearImpactSlippageModel for PnL backtester: depth-aware slippage (#849)
- GMX V2 on-chain position reads via Reader contract (#979)
- PancakeSwap V3 swap lifecycle on BSC (#972)
- Curve StableSwap NG on Optimism (#952)
- Curve adapter extended to Base chain + WETH/cbETH pool (#872)
- SushiSwap V3 swap on BSC + BSC Chainlink price feeds (#878)
- Agni Finance promoted to first-class protocol on Mantle (#827)
- Compound V3 on Optimism + Morpho Blue on-chain market fallback (#851)
- Aave V3 lending lifecycle on BSC (#954)
- Ethena time-warp lifecycle + unstake selector fix (#886)
- LP enrichment methods for Curve receipt parser (#824)
- Pre-built base images for V2 strategy/dashboard deploys (#867)
- Read-only root filesystem container support (#859)
- Anvil watchdog auto-restart for crashed forks (#848)
- Template improvements: looping, funding rates, IntentSequence (#844)
- anvil_funding added to all strat new templates (#957)
- Longer CoinGecko cache TTL for stablecoins (#986)
- PM monitoring: pause/resume, consecutive_errors, pnl, LP fees (#877)
- 17 new demo strategies across Morpho, Compound, Aerodrome, Curve, Aave, SushiSwap, Enso, Fluid, BENQI

### Changed
- Stablecoin symbols extracted to shared constant (#956)

### Fixed
- Startup watchdog no longer kills agents still alive (#998)
- Curve LP position_id returns LP token address, not minted amount (#985)
- Warn when --once loads stale state from previous run (#984)
- Balance retry with backoff and cached fallback (#977)
- Interest_rate_mode wired through all borrow/repay paths (#940)
- Paper trading stability: RPC masking, datetime, anvil_reset (#938)
- Case-insensitive price oracle lookup for mixed-case tokens (#931)
- Slipstream CL swap event parsing in Aerodrome receipt parser (#928)
- Reject flash loan compilation for EOA wallets (#923)
- Curve remove_liquidity slippage via on-chain pool.balances() (#891)
- PancakeSwap V3 extract_swap_amounts uses actual decimals (#908)
- AaveV3ReceiptParser: extract_supply_amounts() (#889)
- Sonic Chainlink feeds + Anvil-only RPC methods (#892)
- Complete BSC Chainlink support (#883)
- Curve CryptoSwap slippage protection via price_ratio (#831)
- Strategy state restored correctly on restart (#865)
- Ghost RUNNING entries: startup reconciliation + heartbeat TTL (#841)
- Block TraderJoe V2 swap: LBRouter2 interface mismatch (#833)
- Quarantine Uniswap V4 swap: fabricated addresses (#832)
- Stale Aave V3 addresses (BSC/Linea) updated (#945)
- Make teardown fallback price fetch chain-aware (#896)
- Populate swap quote fields in dry-run mode (#915)

## [2.5.0] - 2026-03-18

### Added
- Jupiter Lend connector for Solana lending (#785)
- Uniswap V4 wired into IntentCompiler with intent tests (#746)
- Vault intent support in PnL backtester (#778)
- Wrapped token OHLCV proxy fallback with explicit logging (#815)
- Production-ready logging improvements (#799)
- Protocol x chain support matrix expanded: 113 to 131 pairs (#798)
- CI test for adapter config completeness + fix 6 config gaps (#802)
- Pendle min_amount_out logged during swap compilation (#789)
- New demo strategies: Pendle YT yield (#806), Aerodrome RSI (#761), Curve CryptoSwap PnL (#782), Aave V3 PnL lending Polygon (#795), Uniswap V3 RSI sweep (#794), TraderJoe paper trade LP (#783), TraderJoe sweep LP (#766), Solana LST depeg arb (#784)
- `make test-backtest-service` target (#774)

### Fixed
- Tenderly/Alchemy gas estimation: dynamic instead of capping (#817)
- Nonce drift on failed transactions (#805)
- EIP-55 checksums enforced on all static contract addresses (#788)
- Skip simulation for approve-family TXs to prevent Anvil hangs (#775)
- Resolve amount='all' for single intents from decide() (#779)
- Teardown CLI chain config + deterministic retry guard (#821)
- Auto-generate session auth token for standalone gateway on mainnet (#808)
- BSC chain aliases for DexScreener and OHLCV provider (#809)
- Normalize hyphens to underscores in protocol alias resolution (#810)
- Chain added to unwrap intent params (#803)
- Pass --anvil-port through almanak strat run wrapper (#780)
- Wire TraderJoe bin_range from LPOpenIntent protocol_params to compiler (#772)
- Log fallback teardown price failures instead of silently swallowing (#777)
- Explicit is-not-None check for price oracle in all teardown sites (#771)
- Stale GMX V2 Avalanche Reader/SyntheticsReader addresses updated (#787)
- Missing Curve in almanak info matrix (#790)
- ax CLI bugs: gateway noise, --yes flag, standalone crash (#800)
- LP_CLOSE intent added to Aave+Uniswap yield stack teardown (#760)

## [2.4.0] - 2026-03-17

### Added
- Solana chain support with Jupiter, Kamino, Raydium, and Drift connectors (#444)
- BacktestService standalone HTTP API with async submit/poll, paper trading sessions, and fee model export (#616, #634, #741, #742, #740)
- Runner safety components: CircuitBreaker, decide() timeout, StuckDetector wired into live path (#663, #669)
- TeardownManager safety path for runner teardown (#681)
- LP PnL toolkit with impermanent loss estimation, fee tracking, and HODL benchmark (#714)
- 4-source EVM pricing for production resilience (#640)
- Zodiac Roles permission manifest generator with Target[] export format (#598, #648)
- Human-approval actuator for high-value agent trades (#547)
- Protocol aliasing for Uniswap V3 forks, enabling Agni on Mantle (#769)
- Uniswap V4 swap connector skeleton (#725)
- Tenderly simulation support for Sonic, Blast, Mantle, Berachain, Monad (#765)
- Solana LST yield data provider (#724)
- CLI: `strat list`, `status`, and `logs` commands (#644)
- CLI: `info matrix` command for chain/protocol support overview (#707)
- CLI: `--duration` flag for paper trading (#689)
- CLI: `--describe` flag for ax tools argument schema discovery (#688)
- CLI: expose strategy positions via `strat status` (#708)
- `register_token()` convenience API on TokenResolver (#687)
- `warmup_days` parameter for crisis scenarios in indicator strategies (#729)
- Paper trader reads `anvil_funding` from strategy config.json (#728)
- Exit code 2 on SIGTERM so K8s retries preempted pods (#651)
- Centralized gateway PostgreSQL schema (#587)
- BTC.b and sAVAX tokens for Avalanche (#751)
- `StatelessStrategy` base class for strategies that never hold positions (#720)
- Berachain demo strategy and intent compilation tests (#723)
- Pendle and BENQI intent compilation tests (#722)
- New demo strategies: Aave V3 + Uniswap V3 yield stack on Optimism (#756), Aave V3 paper trading on Arbitrum (#732), Aerodrome LP sweep on Base (#738), TraderJoe V2 PnL backtest on Avalanche (#748), Uniswap V3 paper trade on Optimism (#747), Uniswap V3 RSI PnL backtest on Optimism (#739), Aerodrome LP parameter sweep on Base (#733), Solana Narrative Momentum (#744), delta-neutral yield farm (#670), 9 incubating + 3 vault strategies (#593)
- Strategy templates overhaul: 8 to 10 production-ready archetypes with working teardown (#647, #710)

### Changed
- `get_open_positions()` and `generate_teardown_intents()` are now abstract in IntentStrategy; `supports_teardown()` removed - use `StatelessStrategy` for strategies without positions (#720)

### Fixed
- Harden teardown compilation to avoid placeholder prices on mainnet (#764)
- Return hold intent on RSI data failure instead of defaulting to 50 (#770)
- Replace OperatorCard placeholder exposure with real portfolio value (#755)
- Wire funding_rate_provider through IntentStrategy to MarketSnapshot (#754)
- Add amount_in_human / amount_out_human aliases to SwapAmounts (#752)
- Use discounted min_amount_out for Pendle PT/YT sell directions (#750)
- Register BENQI receipt parser in ResultEnricher registry (#745)
- Remove silent 18-decimal fallback in Uniswap V4 adapter (#731)
- Use swapExactPtForToken for Pendle PT sell (#696)
- Add PT-wstETH to token registry and fix silent teardown skip (#695)
- Correct wstETH Chainlink feed on Base and add price ceiling (#692)
- Use actual on-chain position for Morpho Blue withdraw_all (#701)
- Use correct exchange selector for Curve CryptoSwap/Tricrypto pools (#702)
- Account for Curve virtual_price in LP estimation (#581)
- Approve GMX V2 Router for ERC-20 collateral (#713)
- Add GMX V2 Avalanche EventEmitter address (#703)
- Correct SetAuthorization event topic hash in Morpho Blue receipt parser (#549)
- Add integrity validation for gateway token resolution cache writes (#719)
- CoinGecko 429 rate limit resilience for backtesting (#718)
- Guarantee async session cleanup in PnLBacktester (#716)
- Backtest engine QA improvements (#612)
- Validate crisis scenario dates against CoinGecko free tier limit (#641)
- Backtest console/output inconsistencies from QA audit (#642)
- Pre-warm price oracle for flash loan callback tokens (#706)
- Replace hardcoded fallback prices with live market data (#636)
- Downgrade price lookup logs to WARNING for unpriceable derivative tokens (#757)
- Unify token extraction across runner paths (#712)
- Add pre-flight balance check before submitting transactions (#499)
- Add pre-flight balance check for unwrap_native intent (#667)
- Prevent gas-wasting retries on REVERT errors and fix stale balance cache (#694)
- Skip retries for UNAUTHENTICATED and PERMISSION_DENIED gRPC errors (#643)
- Restore wait_for_ready retry loop for gateway startup (#717)
- Wait for gateway readiness with retries before strategy startup (#661)
- Suppress noisy gateway health check logs during startup (#664)
- Downgrade noisy gas estimation and retry warnings to debug level (#666)
- Route gateway logs to stdout and disable ANSI colors in containers (#659)
- Close data provider in warm_cache_async to prevent event loop crash (#638)
- Skip amount='all' resolution in dry-run mode for IntentSequence (#691)
- Resolve amount='all' in flash loan callback intents (#686)
- Alerting production defaults and post-execution balance reconciliation (#682)
- Consolidate V3 LP_CLOSE into universal blocked combo (#656)
- Remove ALCHEMY_API_KEY requirement, use shared RPC config (#628)
- Unify BSC chain naming with central resolve_chain_name (#579)
- Harden Solana integration for production readiness (#637)
- Skip ALMANAK_PRIVATE_KEY in sidecar and safe_zodiac modes (#618, #629)
- Use platform AGENT_ID for lifecycle state writes (#625)
- Add GCP severity field to strategy logs (#700)
- Standardize permissions CLI default to zodiac + permissions.json (#767)
- Add __round__ to RSIData so round(rsi, 2) works in strategies (#602)
- Allow all chains in CLI so bridge works (#597)
- Use keyword args for IntentStrategy instantiation in paper trading (#639)

### Security
- Close compile_intent nested-params policy bypass (#540)

## [2.3.0] - 2026-03-10

### Added
- `almanak ax` CLI for direct DeFi actions from the command line (#583)
- MultiStepStrategy base class for declarative state-machine strategies (#517)
- BENQI lending connector for Avalanche (#528)
- Monad chain support (#515)
- MCP stdio transport server for agent tools (#484)
- Real risk metrics: VaR, Sharpe ratio, volatility, max drawdown (#481)
- Real pre-trade risk validation via `validate_risk` (#482)
- Structured decision tracing for agent tool executions (#485)
- Standardized agent tools error taxonomy with typed enums (#483)
- PendleMarketResolver for dynamic market discovery (#458)
- PolicyEngine runtime state persistence across restarts (#473)
- MockGatewayClient test fixture for agent E2E testing (#471)
- Shared IntentExecutionService converging ToolExecutor and StrategyRunner (#479)
- Structured `iteration_summary` log record (#503)
- `lending_rate()` added to canonical MarketSnapshot (#461)
- Pre-flight ALCHEMY_API_KEY check for archive-RPC chains (#556)
- Nightly market data validation and enhanced Slack reporting (#493)
- 8 new demo strategies: Morpho Blue paper trade (#578), Aave V3 PnL lending (#577), RSI+MACD confluence LP (#576), PancakeSwap V3 LP lifecycle (#575), Balancer flash loan arbitrage (#567), Compound V3 paper trade (#568), TraderJoe V2 ATR-adaptive LP (#551), Aerodrome paper trade (#518)

### Changed
- Strategy metadata (description, chain, strategy_id) moved from config.json to `@almanak_strategy` decorator; config.json now contains only tunable runtime parameters (#591)

### Fixed
- Use ALMANAK_EOA_ADDRESS for safe_zodiac mode instead of derived address (#585)
- Guard Morpho Blue repay against over-repay underflow (#580)
- BENQI receipt parser handles malformed event data gracefully (#571)
- Skip revert diagnostic for compilation failures (#570)
- Guard against zero gas estimate from eth_estimateGas (#569)
- Close data provider sessions after PnL backtest completes (#562)
- Guard CAGR calculation against portfolios losing >100% (#561)
- Use Enso for almanak_rsi teardown to bypass missing price oracle (#553)
- Bump Spark, Aave V3, and BENQI gas estimates to prevent TX reverts (#552, #544)
- Suppress circular import warning on strategy auto-discovery (#543)
- Add OpenZeppelin error selectors to revert decoder (#542)
- Resolve swap_amounts enrichment for Enso and gateway path (#541)
- Correct Arbitrum Curve 2pool USDC address (native -> USDC.e) (#533)
- LiFi and Pendle receipt parsers return SwapAmounts dataclass (#532)
- Add PancakeSwap V3 LP_POSITION_MANAGERS for Arbitrum and Ethereum (#531)
- Add state machine wiring for FlashLoan and Bridge intents (#530)
- Fix Safe wallet address propagation in strategy runner (#529)
- Resolve token addresses to symbols in teardown price prefetch (#526)
- Emit ERROR timeline event on MultiStepStrategy decide() exceptions (#522)
- Remove double slippage in Pendle swap compilation (#521)
- Morpho quick wins: improved error handling (#520)
- Pendle pre-swap routing when tokenIn != tokenMintSy (#516)
- wstETH price resolution on Arbitrum via derived pricing (#514)
- Clean up expired Pendle market in demo strategy (#513)
- Chain-aware native token in errors and placeholder prices (#511)
- QA fixes: teardown CLI, .env template, catch-all anti-pattern (#504)
- Fail compilation on mainnet when no real prices available (#502)
- Classify RPC 'header not found' as connection error (#501)
- Reuse full DictConfigWrapper in teardown CLI (#500)
- Auto-generate session auth token for managed gateway on mainnet (#498)
- Remove hardcoded arbitrum fallback in MarketService (#497)
- Fix dry-run status and backtest CLI command (#496)
- Suppress port-not-freed warning on Anvil shutdown (#491)
- Actionable error for missing state machine wiring (#488)
- Add Sonic chain to framework execution layer (#486)
- Use actual token decimals in Curve extract_swap_amounts() (#466)
- Fix intent state machine gaps (#465)
- Lido stETH approve gas fix and compile_stake_intent tests (#460)
- Lido improvements: gas estimates, error messages (#459)

### Security
- Close compile_intent nested-params policy bypass (#550)

## [2.2.1] - 2026-03-03

### Changed
- `teardown execute` now auto-starts gateway and loads `.env` from strategy directory, matching `strat run` behavior (#477)
- Added `--no-gateway` flag to `teardown execute` to connect to an existing gateway (#477)
- Balance provider injection in teardown CLI so `market.balance()` works during teardown (#452)

### Fixed
- POA middleware injection for Polygon, Avalanche, and BSC chains - strategies no longer crash with `ExtraDataLengthError` (#478)
- `amount="all"` resolution in teardown intents - demo strategies and scaffold template teardown now works correctly (#478)
- Chain-aware Chainlink pricing on mainnet - MarketService no longer defaults to Arbitrum for oracle lookups (#478)
- CLOB bundle routing to ClobActionHandler in single-chain path - Polymarket prediction intents no longer silently fail (#475)
- Clarify ATR `value_percent` returns percentage points, not decimal fraction (#476)
- Broken X/Twitter links updated to x.com/almanak
- Eliminate 177 mkdocs build warnings and update documentation site URL

## [2.2.0] - 2026-03-03

### Added
- Curve Finance swap and LP support wired into intent compiler (#403)
- Velodrome V2 (Optimism) addresses added to Aerodrome connector for cross-chain Solidly-fork support (#412)
- `--teardown-after` CLI flag to auto-close positions after `--once` runs (#416)
- Live on-chain Aave V3 supply/borrow rates via `market.lending_rate()` (#404)
- Live on-chain Compound V3 lending rate fetching (#430)
- `MarketSnapshot.collateral_value_usd()` helper for perp position sizing (#424)
- Interactive platform selector for `almanak agent install` when no platform is auto-detected (#407)
- Nightly Market Data API contract tests and 4 new indicator calculators: ADX, OBV, CCI, Ichimoku (#442)
- Multi-language documentation: Mandarin, French, Spanish translations (#418)
- `/release` skill for automated changelog, tagging, and GitHub release creation

### Changed
- Renamed public repo references from almanak-sdk to sdk (#467)
- Removed ClawHub marketplace references; OpenClaw platform support retained

### Fixed
- Prevent $6.14B wstETH price via magnitude outlier detection in price aggregator (#401)
- Suppress spurious amount-chaining warnings for single intents (#443)
- Pre-fetch prices in teardown path to avoid placeholder fallback (#437)
- Load .env in backtest commands (#453)
- Patch _version.py during release so CLI reports correct version (#448)
- Harden Anvil fork lifecycle and fix flaky intent tests (#417, #441)
- Correct Polygon WETH balance slot from 3 to 0 (#415)
- Accurate revert diagnostic for compilation failures (#414)
- Skip simulation estimation for non-first TXs in multi-TX bundles (#402, #421)
- BorrowIntent summary shows actual amounts instead of N/A (#427)
- GMX V2 receipt parser: correct event topic hashes and EventEmitter matching (#423)
- Prevent 30s gateway timeout during Aerodrome LP_CLOSE compilation (#408)
- Defer Polymarket warning from init to compile time (#406)
- Gas price cap quick wins (#405)
- Receipt parser logs tx=N/A, 0 gas (#410)
- Transfer-based fallback for Aerodrome LP_CLOSE lp_close_data (#409)

## [2.0.0] - 2026-02-28

First public open-source release of the Almanak SDK.

### Added
- **Intent-based strategy framework** with 19 intent types (Swap, Hold, LP Open/Close, Borrow, Repay, Supply, Withdraw, Stake, Unstake, Perp Open/Close, Flash Loan, Prediction Buy/Sell/Redeem, and more)
- **26 protocol connectors**: Uniswap V3, SushiSwap V3, PancakeSwap V3, TraderJoe V2, Aerodrome, Curve, Balancer, Aave V3, Morpho Blue, Compound V3, Spark, Lido, Ethena, Pendle, GMX V2, Hyperliquid, Polymarket, Kraken, Enso, LiFi, and others
- **12-chain support**: Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain
- **Dual backtesting engine**: PnL backtester (historical price simulation) and Paper Trader (live-like execution on Anvil forks), with parameter sweeps, Monte Carlo, walk-forward optimization, and crisis scenario testing
- **Gateway architecture**: Secure gRPC sidecar holding all secrets, with strategy containers running user code in isolation
- **CLI tools**: `almanak strat new`, `almanak strat run`, `almanak strat backtest`, `almanak gateway`, with auto-managed Anvil and gateway lifecycle
- **17+ demo strategies** covering DEX trading, LP management, lending, perpetuals, prediction markets, CEX integration, yield farming, and copy trading
- **Multi-language documentation** site at docs.almanak.co (English, Mandarin, French, Spanish)
- **AI agent skills**: Strategy builder skill for Claude Code, Codex, Cursor, Copilot, and 6 more platforms via `almanak agent install`
- **Non-custodial Safe design**: Fine-grained permission controls through Zodiac Roles Modifier, user maintains full control of funds
- **Three-tier state management**: Automatic HOT/WARM/COLD persistence for strategy state
- **Production services**: Alerting (Slack/Telegram), stuck detection, emergency management, canary deployments
