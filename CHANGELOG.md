# Changelog

All notable changes to the Almanak SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2.16.0] - 2026-05-22

### BREAKING — VIB-4281 Retired PAUSE / RESUME lifecycle commands

The `PAUSE` and `RESUME` lifecycle commands have been removed from the gateway's
`LifecycleService`. The runner accepts only `STOP`; the V2 platform's pause and
resume endpoints return `410 Gone`. The three-action UX model is **Stop** (kill
pod, leave positions, hits `/v2/agent/terminate`), **Teardown** (unwind then
exit, hits `/v2/agent/stop`), and **Emergency Stop** (`kubectl delete`).

The `PAUSED` state is no longer in the gateway's writable vocabulary, but
historical `agent_state` rows still readable for backwards compatibility.

Local SDK behaviour: `almanak strat pause` / `almanak strat resume` CLI surfaces
still return success at the CLI layer (they route through
`DashboardService.ExecuteAction`, not the lifecycle channel) but the queued
PAUSE/RESUME row is now silently dropped by the runner with a `WARNING` log.
Scripts that called `almanak strat pause --wait` will time out waiting for a
`PAUSED` status that never arrives. Use `almanak strat stop` instead; a future
PR will remove the broken CLI surfaces. Direct callers of
`LifecycleService.WriteCommand` (PAUSE / RESUME) or `WriteState` (PAUSED) at
the gRPC layer do now receive `INVALID_ARGUMENT`.

Migration guidance:
- Restart a stopped strategy with `restartAgent` / `/v2/agent/restart`
  (cached-image redeploy). Stop + Restart reconstructs the same in-memory
  state from `metrics_db` rows.

### BREAKING — VIB-4062 Unified MarketSnapshot

The two `MarketSnapshot` classes that have silently diverged since the
v1→v2 framework migration on 2026-01-26 are unified into a single canonical
class at `almanak.framework.market.snapshot.MarketSnapshot`.

**Imports that continue to work**:
- `from almanak import MarketSnapshot`
- `from almanak import MultiChainMarketSnapshot` (TypeAlias to MarketSnapshot)

**Imports that now hard-fail with `ImportError`**:
- `from almanak.framework.strategies import MarketSnapshot`
- `from almanak.framework.data.market_snapshot import MarketSnapshot`

**Imports that still work but are DISCOURAGED** (kept as transitional re-exports
to soften the upgrade for existing strategies; will be removed in a future
release):
- `from almanak.framework.strategies.intent_strategy import MarketSnapshot`
- `from almanak.framework.strategies.intent_strategy import MultiChainMarketSnapshot`
- `from almanak.framework.strategies.multichain import MultiChainMarketSnapshot`

See the migration guide at `docs/migration/vib-4062-marketsnapshot.md`. The
PRD at `docs/internal/PRD-MarketSnapshotFix.md` is the implementation
source-of-truth.

**Other changes**:
- Multi-chain snapshots raise `AmbiguousChainError` on `chain=None` (was: silent
  default-to-primary). Single-chain snapshots raise `ChainNotConfiguredError`
  on a chain= mismatch (was: silent ignore).
- `fork_rpc_url` is neutered on the production strategy surface: the property
  remains on `MarketSnapshot` for compatibility but returns `None` outside
  paper trading. Paper trading routes fork-aware reads via internal service
  adapters. Strategies relying on this attribute for production logic will
  now silently receive `None`.
- New public `seed_*` API on `MarketSnapshot` (legacy `set_*` retained as
  aliases until deprecation).
- New `MarketSnapshotBuilder` with named factories per runtime surface
  (`for_strategy_runner`, `for_pnl_backtest_state`, `for_paper_fork`,
  `for_http_backtest_spec`, `seeded`).
- Every snapshot now carries `runtime_surface` ∈ `{"local_sdk", "hosted",
  "pnl_backtest", "paper_fork", "http_backtest", "unit_test"}`.
- Layered drift-prevention CI gates: AST uniqueness, public-surface lockfile,
  behavioral return-type contract suite, identity assertion under multiple
  import orders, caller-bifurcation anti-bypass, private-cache-write
  anti-bypass, direct-constructor anti-bypass, dynamic-import discovery
  sweep, lean-import regression test.

### Added

#### Connectors & chains
- Camelot DEX connector (folded into connector framework alongside Fluid) (#2360)
- Register `aave_v3` on bnb / mantle / xlayer chains (VIB-4345) (#2272)
- Register `balancer` on avalanche (VIB-4346) (#2271)
- Land 3 deferred chain×connector entries + ethereum traderjoe_v2 LP test (VIB-4419) (#2312)
- Register `stargate` bnb USDT BRIDGE chain (VIB-4354) (#2277)
- Register `uniswap_v3` on monad + bnb chains (VIB-4349, VIB-4350, VIB-4351) (#2274, #2275, #2276)
- Register `traderjoe_v2` on bnb, ethereum, arbitrum (VIB-4374–4378) (#2304, #2306, #2307, #2308)

#### Other
- Per-test gateway sidecar — `managed_serve` entrypoint, `test_controller` HTTP service, `strat test --no-gateway` flag, anvil balance-cache fix (#2351)
- Gateway `PoolAnalyticsService` + framework thin-client (VIB-4727) (#2389)
- `multi_lp_dual_range` demo — reference template for multi-position LP dispatch (#2388)
- Uniswap V4 LP accounting end-to-end V0 (VIB-4426): `lp_v4` fixture + Anvil-Base E2E proof, canonical V4 PoolKey seed registry, V4 `extract_lp_open_data` gateway PoolKey lookup, V4 connector hygiene (#2335, #2339, #2340, #2341, #2342)
- Accounting QA framework (matrix harness + protocol-agnostic algos) (VIB-4316) (#2257)
- Layer 5 typed accounting events for TA, LP, Looping (VIB-4085 / VIB-4086 / VIB-4087) (#2161)
- Accountant Test cell #22 registry coherence (VIB-4201 / T15) (#2221)
- UniV3 LP registry-mode cutover (VIB-4198 / T12) (#2214)
- L0 persistence-invariant sweep (VIB-4193 / T07) (#2215)
- `position_reference` JSON shape on accounting_events (VIB-4196 / T10) (#2211)
- Stamp per-primitive `primitive_version` on every event (VIB-4166) (#2206)
- `MatchingPolicy.for_primitive()` typed accessor (VIB-4195) (#2204)
- `Intent.registry_handle` reserved field (VIB-4192) (#2205)
- Reclassify BRIDGE → TRANSFER + gateway whitelist (VIB-4164) (#2196)
- Category-handler registry + transfer stub (VIB-4163) (#2194)
- Canonical primitives taxonomy module (VIB-4161) (#2181)
- Five placeholder IntentType values + fail-fast compiler guard (VIB-4165) (#2199)
- `agent_tools` PolicyEngine refuses placeholder primitives (VIB-4167) (#2203)
- Aerodrome Slipstream local readiness W1+W2+W3 (VIB-4434) (#2331)
- Aave V3 pre-state `e_mode_category` + `interest_rate_mode` (VIB-4213 / T27) (#2286)
- PancakeSwap V3 LP — `extract_lp_open_data` + 1-pos & 2-pos fixtures (#2248)
- SushiSwap V3 LP — `extract_lp_open_data` + 1-pos & 2-pos fixtures (#2247)
- Aerodrome Slipstream LP — `extract_lp_open_data` + 1-pos & 2-pos fixtures, E2E Anvil verified (#2241)
- `lp_triple` fixture — order-invariant 3-LP position tracking (VIB-4185) (#2244)
- `lp_dual` fixture (2 LP positions, basis-pool FIFO) (#2228)
- Compile-time borrow capacity pre-flight for lending intents (#2129)
- Intent-coverage gate (opt-out warn-only → enforce by default) — VIB-4298 / VIB-4303 (#2246, #2263)
- Intent-coverage excused YAML (50 structural cells) (VIB-4309) (#2261)
- Intent-coverage backlog (98 tests + 3 fixes) (VIB-4307) (#2253)
- 9 BridgeIntent tests + retire LiFi BRIDGE / FlashLoan / aggregator SWAP (VIB-4341, VIB-4309) (#2267)
- AST attribution for BridgeIntent + FlashLoanIntent (VIB-4340) (#2264)
- `ConnectorRegistry` foundation (VIB-4302 / VIB-4298 PR 1) (#2242)
- Aerodrome §8 follow-up bundle (W5+W6+W7+W8) (VIB-4468) (#2333)
- VIB-4285 factory UX + 3 mixed-primitive accounting fixtures + dashboard UX (#2237)
- VIB-4488 Morpho post-merge cleanups (4 tickets) (#2343)
- VIB-4488 Morpho looping G15 cell + L3 HF guard (16/21 → 17/21+) (#2336)
- DashboardService RPCs + reconciliation primitives (VIB-4493 Phase 1) (#2337)
- Dashboard template-renderer scaffold + double-title fix (#2352)
- Dashboard hosted-parity single-strategy entrypoint (Problem A1) (#2372)
- Dashboard gateway-backed Positions + Lifecycle wired into LP template (Problem A2) (#2373)
- Dashboard TA chart subplot — `prepare_ta_session_state` for OHLCV + RSI + buy/sell markers (#2368)
- Dashboard position alias, positions table, range-history plot (#2326)
- Dashboard position-value fixes for `lp_dual` audit + multi-position panel (#2290)
- Dashboard trade-tape readability — LP +/fees, failure reason, datetime, TA exports (#2160)
- Dashboard reconciliation tab wired + delete unverified-lane scaffolding (VIB-4548) (#2348)
- Dashboard authoring docs (#2177)
- 5 framework dashboard templates bake in 3 accounting sections (#2176)
- RSI custom dashboards (4 strategies) (VIB-3975) (#2325)
- OHLCV single composition path — factory + dashboard API (VIB-4347) (#2270)
- Gateway announces `INITIALIZING` state from strategy-pod gateway (#2310)
- Gateway stamps running almanak version on `agent_state` writes (#2138)
- Gateway `SumLedgerGasUsd` RPC (#2255)
- Postgres state RPCs (hosted half of `SaveLedgerAndRegistry`) — VIB-4205 / T19 (#2239)
- Gateway migration_state RPCs + cutover boot guard (SQLite half) — VIB-4208 / T22 (#2230)
- `RegistryAutoCollisionError` typed exception (VIB-4200 / T14) (#2222)
- `save_ledger_and_registry` atomic commit primitive (local SQLite) — VIB-4197 (#2207)
- Blueprint 28 + `position_registry` schema (VIB-4188 + VIB-4190) (#2197)
- `PositionService.Reconcile` + `ax positions reconcile` CLI (VIB-4210 / T24) (#2240)
- Hosted-mode Postgres teardown backend + collapse `is_hosted` forks (VIB-4049) (#2234)
- Hosted teardown state routed through gateway (VIB-4317) (#2258)
- `accounting-timeline` rescoped to UX activity feed (VIB-4039 epic) (#2117)
- `portfolio_snapshots` Phase 4 identity end-to-end (VIB-4091, 8 tickets) (#2162)
- Config service Phase 6 — typed `AgentToolsConfig` + `FrameworkConfig` submodels migrate framework env reads (#2156)
- Config service Phase 5 — typed submodels replace 120 env reads (#2152)
- Config service Phases 0–3 — skeleton, lint gate, gateway boot cutover, Click options helper, strategy schema (#2107)
- Migrate demo `run_anvil` env reads to config service (VIB-4425) (#2328)
- Migrate gateway env reads to config service (VIB-4424) (#2324)
- Migrate framework/service env reads to config service (VIB-4423) (#2313)
- ALM-2725 report running Almanak version (#2168)
- `/pr-merger` Stage 6.5 step-back drift check skill (VIB-4141) (#2173)
- UAT-GATE v2 + pr-merger blocked-PR diagnosis + targeted kitchenloop skills (#2171)
- `kitchenloop` prd-shred phase for large architectural PRDs (#2198)
- `kitchenloop` demo-gate re-run gate before PR creation (VIB-4181) (#2190)
- `accountant` Blueprint 27 v2 rewrite + native-gas in PnL (VIB-4224 ACC-01 + VIB-4225 ACC-02) (#2208)

### Changed

- Fold Uniswap V3 compiler into connector (#2350)
- Fold Curve compiler into connector (#2354)
- Fold Fluid / Camelot compilers into connectors (#2360)
- Fold phase 2 compilers into connectors (#2375)
- Coverage W1 — accounting category-handlers + basis decomposition (VIB-4078) (#2145)
- Coverage W2 — gateway services: simulation, funding-rate, dashboard (VIB-4079) (#2149)
- Coverage W3 — CLI hotspots Phase 4 follow-on extractions (VIB-4080) (#2153)
- Coverage W4 — dashboard scope clarification (VIB-4081) (#2154)
- Coverage W5 — backtesting long tail: dead code + paper/engine + risk/reconciler (VIB-4082) (#2157)
- Coverage W6 — Pendle compilers Phase 2 follow-on (VIB-4083) (#2158)
- Ratchet `fail_under` floor 72 → 75 (e64c8f028)
- Audit-engine consolidation + Empty≠Zero deletion (VIB-4228) (#2201)
- Re-point 4 consumers at `primitives.taxonomy` + per-primitive `matching_policy_version` (VIB-4162) (#2192)
- Split CDP/LIQUIDATION from LENDING placeholder mapping (VIB-4248) (#2209)
- Per-protocol extraction-spec overlay (VIB-4320) (#2269)
- Config service Phase 4b — private key via kwarg, no `os.environ` mutation (#2111)
- Config service Phase 4c — centralise `ALMANAK_STRATEGY_FOLDER` mutation (#2112)
- Retire PAUSE/RESUME commands and PAUSED state (VIB-4281) — see BREAKING (#2266)
- VIB-4218 / T18a — UniV3 LP 21-cell post-T12 baseline (#2219)
- Layer 5 Uniswap V3 LP assertions added (#2359)
- Layer 5 Uniswap V4 LP assertions (VIB-4594) (#2369)
- Layer 5 TraderJoe V2 LP assertions (VIB-4598) (#2366)
- Layer 5 Morpho Blue lending assertions (VIB-4604) (#2367)
- Layer 5 Compound V3 lending assertions (VIB-4603) (#2365)
- Layer 5 Aerodrome + Slipstream LP assertions (VIB-4597) (#2364)
- Layer 5 Aave V3 lending assertions (VIB-4593) (#2361)
- Layer 5 SushiSwap V3 LP assertions, all chains (VIB-4595) (#2363)
- Layer 5 PancakeSwap V3 LP assertions, all chains (VIB-4596) (#2362)
- Intent-tests for traderjoe_v2 SWAP / LP_OPEN / LP_CLOSE across bnb / ethereum / arbitrum (VIB-4371–4378) (#2300–#2308)
- Intent-tests for uniswap_v4 SWAP / LP_OPEN / LP_CLOSE / LP_COLLECT_FEES across base / optimism / polygon / avalanche / bnb (VIB-4355–4373) (#2280–#2302)
- Intent-tests for pancakeswap_v3 base SWAP + LP_OPEN/CLOSE/COLLECT_FEES (VIB-4352, VIB-4353) (#2278, #2279)
- Intent-tests for uniswap_v3 monad + bnb + avalanche LP / SWAP / COLLECT_FEES (VIB-4348, VIB-4349, VIB-4350, VIB-4351) (#2273–#2276)
- Optimism aerodrome SWAP intent tests + Zodiac permissions (VIB-4389) (#2319)
- Optimism aerodrome LP_OPEN + LP_CLOSE intent tests (VIB-4390) (#2318)
- xfail fork-pin flakes (VIB-4314, VIB-4590) (#2355)
- Restore sushiswap_v3 SWAP coverage (#2136)
- BNB pancakeswap_v3 insufficient-balance compile failure intent test fix (#2151)
- Switch xlayer Uniswap V3 swap to USDT0/USDG (#2133)
- VIB-4199 / T13 — bug #2130 acceptance test (local-mode) (#2224)
- VIB-4216 — anti-bypass guard for open-position queries (T30) static test (#2210)
- VIB-4194 — UniV3 L1 offline goldens incl. `expected_registry_row.json` (T08) (#2212)
- T02 Tier-1 parser-coverage audit (VIB-4187, Hard Gate 1) docs (#2200)
- Improve intent-test CI RPC proxy caching (#2121)
- Sync hand-written docs pages with current SDK state (#2142)
- Migration spec — T04 Hard Gate 3 cutover for `position_registry` (VIB-4189) (#2202)
- Catch primitives + position-registry blueprint docs up to 2026-05-11 main (#2229)
- Blueprints + docs + skill sync to 2026-05-12 main (audit-driven catch-up) (#2243)
- AGENTS.md slim from 556 to 423 lines (#2220)
- AGENTS.md — refactor (not allowlist) is the default when CRAP trips (#2134)
- AGENTS.md — drop uv references; add `almanak strat test` (#2137)
- E2E flow non-negotiables — add uv-run prefix (#2226)
- Lending pre/post-state pipeline blueprint + QA cleanup (VIB-3474 shipped) (#2217)
- Boundary doc — T01 accounting × position registry (VIB-4186) (#2195)
- VIB-4426 V0 last-mile session report + scalability proof (#2344)
- VIB-1939 audit — close as resolved + mark W9 done (#2334)
- VIB-4299 config-service Phase 7 completion record (#2329)
- Strategy-layer docs — multi-position dispatch one Intent per iteration (#2387)
- Dashboard docs — canonical `api_client` kwarg in LP examples + regression test (#2380)
- Dashboard plots package exports + blueprint 23 alignment with actual API (#2155)
- Dashboard anatomy split into template vs custom paths (#2180)
- Delete blueprint 09; consolidate dashboard docs into 22 + 23 (#2159)
- Connector additions documented as required step — ConnectorRegistry (#2245)
- UAT-card test inventory restored (17→41) (VIB-4210) (#2249)
- CRAP refactor protocol skill — blueprint-first, Plan-agent handoff, test baseline (#2178)
- `pr-merger` Stage 4 skill — name CI checks where easy fix is wrong fix (#2175)
- Bump almanak-code to v1.0.13 (#2144)
- Bump almanak-code to v1.0.15 (#2179)
- Misc cleanup — salvage stranded commits + preserve history + drop 14 superseded notes (#2174)
- Cleanup batch — remove 341 stale internal docs/notes (#2001)
- Drop stale MarketSnapshot import paths post-VIB-4062 (#2172)
- Strip Linear ticket refs from user-visible strings (#2163)
- CI — notify platform repo after RC artifacts publish (#2268)
- CI — backfill_runtime_images workflow for older SDK releases (#2135)
- CI — refresh `runtime-image-prepull` DaemonSet after every release (#2139)
- CI — fix `gke-gcloud-auth-plugin` install on apt-managed gcloud (#2143)
- Kitchen loop iter 177 / 178 / 179 artifacts (#2184, #2186, #2188)
- Kitchen loop hotfix tickets in Todo + Triage rescue (VIB-4179) (#2189)
- Wave 1 accounting/portfolio/teardown/simulator fixes (VIB-4581/4584/4587/4588) (#2353)
- F1 PRD artifacts + Codex P2/P4 fixes (VIB-4159 follow-up) (#2213)
- Codex-reproduced bug bundle — VIB-4178 + VIB-4310 + VIB-3210 (#2256)
- Add `crap-diff-fresh` make target for CI-parity local CRAP gate (#2218)
- Dependency updates:
  - bump `actions/cache` from 4 to 5 (#2059)
  - bump `google-github-actions/setup-gcloud` from 2 to 3 (#2164)
  - bump `gitpython` from 3.1.47 → 3.1.49 (#2167)
  - bump `gitpython` from 3.1.49 → 3.1.50 (#2193)
  - bump `mako` from 1.3.11 → 1.3.12 (#2166)
  - bump `urllib3` from 2.6.3 → 2.7.0 (#2238)

### Fixed

- Submitter receipt-recover from "nonce too low" to prevent zombie positions (#2358)
- LP_CLOSE fees-vs-principal conflation on UniV3 / PancakeSwap V3 (#2385)
- Block-anchored lending post-state reads (VIB-4589 / F7) (#2357)
- Stale gateway balance cache + S2 preflight design (VIB-4613, VIB-4614) (#2356)
- `SwapEventPayload` tolerates unmeasured `amount_in` / `amount_out` (VIB-4490, G6 unblock) (#2338)
- Wire Track-C `position_state_snapshots` through gateway (VIB-4541) (#2347)
- Drop WETH from `lp_v4` anvil_funding + re-baseline (VIB-4538) (#2345)
- Fall back to TokenResolver for Aave V3 reserves missing from static registry (#2327)
- Re-baseline `lp-uniswap_v4-base` LP4 PASS → XFAIL (VIB-4426) (#2332)
- Morpho looping repay liquid wallet balance before WITHDRAW (#2330)
- Morpho SUPPLY consumes `supply_collateral_amount` (VIB-4437, MorphoMay15 F2) (#2322)
- Morpho pre-state resolves loan_token from registry for SUPPLY / WITHDRAW (VIB-4432) (#2321)
- PriceAggregator fails closed on 2-source divergence (VIB-4439, MorphoMay15 F1) (#2323)
- Silence misleading "free public RPC" log (VIB-4429) (#2317)
- Avoid doubled "dashboard" in loading spinner label (#2314)
- Hosted strategy dashboards general improvements (#2390)
- Token-decimals-aware tape formatter (WBTC dust) (VIB-3890) (#2371)
- `test_controller` pre-sets `ALMANAK_GATEWAY_ALLOW_INSECURE` so `managed_serve` subprocess boots (#2374)
- Runner amount-chaining warning gated on chained 'all' usage (VIB-2036) (#2370)
- Codex fix — looping demo teardown sequences (#2316)
- Codex fix — demo teardown routes (#2048)
- Safe slot probing + revert-reason decoding for FiatToken-proxy funding (#2283)
- LP payload `pool_address` stored V3 descriptor, not on-chain address (VIB-4396) (#2289)
- V4 collect-fees flake from live oracle ↔ fork-block coupling (VIB-4427) (#2315)
- `uniswap_v3` `extract_liquidity` reads wrong slot; strict uint128/uint160 decoding (VIB-4395) (#2288)
- Aerodrome `extract_registry_payload_open/close` so `position_registry` fills (VIB-4305) (#2251)
- `swap_handler` resolves address-keyed `token_in` to symbol for `price_inputs_json` lookup (VIB-4304) (#2250)
- PostgresStore write path for `position_events` (VIB-4315) (#2254)
- `state_service` asyncpg datetime binding in `UpdateMigrationState` / `MarkBackfillComplete` (VIB-4313) (#2252)
- Augmentation chokepoint reads `position_registry` (VIB-4278, closes L5_22) (#2236)
- Guard against pool-descriptor strings at LP consumer sites + producer sweep (VIB-4274) (#2231)
- `lp_dual` explicit `registry_handle` per leg (VIB-4279) (#2233)
- Wire LP wallet-basis hooks (VIB-4262, G6 reconciliation closer) (#2225)
- Populate lending `_before` fields from `pre_state_json` (VIB-4257) (#2223)
- Merge intent-token prices into teardown ledger oracle (VIB-4318) (#2260)
- Emit `il_usd` and `hodl_value_usd` on LP close (VIB-4319) (#2259)
- Rotate Pendle test fixture to live YT-sUSDe-13AUG2026; flag expiries in intent tests (#2235)
- CoinGecko fail fast on 429 with one bounded 1s retry instead of compounding backoff (#2232)
- CI mirror BNB fork-block pin to framework-canonical BSC env var (VIB-4003) (#2140)
- State machine — classify host-unreachable RPC errors as permanent (VIB-1215) (#2187)
- State machine — expand non-retryable keywords for market/pool/Drift errors (VIB-2866) (#2182)
- Submitter — decode TraderJoe V2 custom error selectors (VIB-3102) (#2183)
- Demo-runner — clear `ALMANAK_CHAIN`/`CHAINS` in `run_demo.py` subprocess env (VIB-4177) (#2185)
- `pr-manager` waits for maturing PRs in post-batch loop (VIB-4180) (#2191)
- `_fund_anvil_wallets` honors `settings.private_key` fallback (#2170)
- Expose Quant Data Layer methods on canonical MarketSnapshot (ALM-2696) (#2125)
- `pcs-v3` swap parser preserves "Empty != zero" (#2127)
- Teardown retries init under WAL contention; surface init failures (ALM-2705) (#2119)
- Detect stale CEX upstream + failover to keep RSI fresh (ALM-2697) (#2120)

### Security

- PriceAggregator fails closed on 2-source divergence — prevents single-source manipulation of LP/oracle prices (VIB-4439) (#2323)

## [2.15.0] - 2026-04-23

### Added

- Aerodrome Slipstream (CL) connector for Base - full CL LP lifecycle: `lp_open`, `lp_close`, `collect_fees`, plus `aerodrome_slipstream_lp` demo strategy (VIB-3321) (#1688)
- `StrategyDataRequirements` - strategies declare data dependencies; runner skips unused fetches (VIB-3392) (#1821)
- Generalized ERC-4626 vault dispatch - `Intent.vault_deposit` / `vault_redeem` now works with any compliant vault, not just MetaMorpho (VIB-3363) (#1795)
- `SaveLedgerEntry` RPC on `StateService` - structured trade records persisted to the transaction ledger (VIB-3201) (#1794)
- On-chain Zodiac Roles permission verification harness with SWAP, LEND, and LP dispatch coverage (#1819, #1822, #1823, #1824)
- `DATA_ERROR` escalation - HOLD cycles caused by critical market data failures now surface as `DATA_ERROR` instead of counting as silent no-ops (#1810)
- `ax resolve` expanded via Pendle API + auto-spawns gateway if none is running (#1772)

### Fixed

- Token aliases: ETH and DAI.E resolved to canonical on-chain addresses (#1747); BTC aliased to BTCB on BSC (#1796); POL preferred over MATIC for Polygon native (#1820)
- Polymarket now routed entirely through the gateway - no direct network calls from strategy container (#1808, #1816)
- Gateway: `CancelledError` unmasked in audit wrapper; dynamic token resolution restored (#1769)
- Runner: stuck-resume now runs before the circuit-breaker gate for multi-chain strategies (#1674)
- Runner: reconciliation enforcement gated behind config flag (defaults to observation mode) - VIB-3158 / VIB-3348 (#1803)
- Runner: `ACCOUNTING_FAILED` duration includes snapshot-phase time; `_consecutive_errors` double-count eliminated (#1777, #1786)
- Execution: `_emit_event` None guard; error propagated from receipt in `_phase_enrich`; `_init_pipeline_state` failures routed through exception handler (#1669, #1670, #1671)
- GMX perp lifecycle: teardown state transition and USD price resolution (#1644)
- Simulator: `eth_estimateGas` now times out instead of hanging indefinitely (VIB-3295) (#1645)
- Dashboard: 8-issue latent bug bundle (#1750)
- CLI: latent bug bundles in run, status, and backtest helpers (#1689, #1695, #1744, #1754)
- Gateway: OKX data extraction latent bugs (#1760)
- Uniswap V4 direction fallback; ledger `Decimal(0)` init (#1774)
- Gateway client pre-validated before strategy loop starts (#1676)

## [2.14.0] - 2026-04-21

### Added

- Curvance isolated lending connector on Monad with 2 markets and intent-compiler wiring (#1563)
- Aster Perps connector extracted from `pancakeswap_perps` into canonical `aster_perps` package with `broker_id=0` (raw Aster attribution); `pancakeswap_perps` becomes a thin compatibility shim defaulting `broker_id=2` and emitting a `DeprecationWarning` once per process (#1547)
- `aster_perps_basic` demo strategy: 3x BNB/USD long with open + close round-trip on BNB Anvil fork (#1547)
- `PerpCloseIntent.position_id` field: close Aster positions directly through the IntentCompiler instead of the direct-SDK workaround; bytes32 validation enforced at intent construction (#1547)
- Aster Perps 4-layer intent tests: close-via-intent, keeper settlement with broker=0 attribution, min-notional rejection (#1553)
- Aster DEX Phase 2 research artifacts in `docs/internal/discussions/` (PRD, research brief, RQ-1 Diamond-topology memo) (#1552)
- `edge_polymarket_megaeth_tail` demo strategy with `max_price` threading (#1565)
- `almanak strat pnl -s` dedicated PnL view (#1599)
- `almanak strat check` preflight command (#1572)
- `almanak ax lp-list`, `lending-list`, `portfolio` read commands (#1536)
- `almanak ax lending-supply`, `lending-borrow`, `lending-repay`, `lending-withdraw` shortcuts with quieter CLI output (#1535)
- Persistent compiled-bundle cache across `ax` CLI invocations (#1538)
- `--chain` flag on `ax balance`, `price`, `swap`, `unwrap` (#1583, #1594)
- `ALMANAK_CHAIN` env var overrides `config.json` chain (#1550)
- Config-driven perps direction (LONG/SHORT) in scaffolded strategies (#1574)
- Deeper auto-generated tests for scaffolded strategies (#1578)
- `StrEnum` state machines in scaffolded templates (#1576)
- `get_status()` enriched per template with state/pnl/health (#1600)
- `validate_config()` lifecycle hook on `IntentStrategy` (#1571)
- Unified `HealthFactorProvider` for Aave / Morpho / Compound (#1575)
- Protocol-aware variant resolution in `market.balance()` (e.g. aUSDC vs USDC) (#1582)
- Protocol-fee extraction interface on `ResultEnricher` (#1602)
- BRIDGE result enrichment with typed `BridgeData` (VIB-3226) (#1628)
- Realized `slippage_bps` populated across the swap slippage path and all receipt parsers (VIB-3203) (#1601, #1606)
- Dynamic gas-worthiness gate in the data layer (VIB-3177) (#1577)
- USD-denominated per-tx value cap in the execution path (VIB-3133) (#1568)
- Real pre/post balance reconciliation in the runner with `SwapIntent` enforcement (VIB-3158) (#1570)
- Mandatory accounting persistence in live mode (VIB-3157) (#1585)
- Atomic state writes (fsync+rename) across all backends (VIB-3156) (#1584)
- Compile-time (market, collateral) validation in GMX V2 perps compiler (VIB-3173) (#1573)
- Compile-time (market, collateral) validation in Drift perps compiler (VIB-3202) (#1596)
- PnLAttributor v2 with real IL + `fee_pnl` attribution (VIB-3205) (#1604)
- 12 previously-missing connectors surfaced in `almanak info matrix` (#1566)
- Prediction-market category added to `almanak info matrix` with Polymarket (VIB-3139) (#1579)
- `backtest pnl` auto-discovers a local `strategy.py` when `-s` is omitted (VIB-2917) (#1542)
- Connector RPC/HTTP routed through the gateway with retry policy (enforces the gateway boundary) (VIB-2986) (#1533)
- Compile-time RPC reads routed through the gateway (#1618)
- SPL mint RPC fallback for long-tail Solana tokens (VIB-2990) (#1534)

### Changed

- Fail-closed semantics in the compiler: pool validation, price-impact checks, tick math (VIB-3160) (#1587)
- Fail-closed `ResultEnricher` with a three-variant extract contract (VIB-3159) (#1586)
- Chain-aware token resolution and pricing across the data layer (#1613, #1616)
- Placeholder price fallbacks removed from swap adapters (fail on missing oracle instead of pricing wrong) (VIB-3134) (#1589)
- Polymarket requires explicit price anchors and fails fast on missing env vars (VIB-3131, VIB-3132) (#1567)
- `backtest pnl` `total_return_pct` now reports actual percentage (33 = 33%) instead of a decimal fraction (VIB-2915) (#1541)

### Fixed

- Aerodrome `LP_CLOSE` accepts a bare pool address as `position_id` (#1632)
- Demo LP teardown state recovery (#1629)
- `morpho_looping` `_total_collateral` updates on SUPPLY and persists across restarts (VIB-3297) (#1641)
- Demo Aave Arbitrum strategies switched from WETH to wstETH (VIB-3294) (#1643)
- Runner reconciliation treats an empty mismatch list as success (VIB-3292) (#1640)
- Gateway `MetricsInterceptor` initializes status before awaiting (VIB-3293) (#1642)
- BridgeIntent destination fields resolved in the runner (VIB-3223, VIB-3224) (#1614)
- Bridge-wait poll catches all exceptions so the failure callback always runs (#1651)
- Bridge source-TX verification no longer uses a direct Web3 client; routes through the gateway (#1653)
- `leverage_loop_cross_chain`: experimental warning, stall fix, live-position tracking (#1654)
- Slippage-breach timeline event sets `last_execution_result.error` before emitting (#1652)
- Token symbol extraction strips pool-type suffixes (volatile/stable/cl) to stop noisy oracle misses (#1658)
- Native-in swaps skip the ERC20 allowance step (VIB-3135) (#1592)
- Compiler price oracle expanded with wrapped/native aliases (VIB-3136) (#1580)
- Polygon MATIC<->POL alias added; hardcoded price fallbacks removed (VIB-3137) (#1591)
- Runner `_TYPE_MAP` covers `lp_collect_fees` and `vault_deposit` (VIB-3183) (#1593)
- Runner `wrap_native` added to `inner_runner._TYPE_MAP` (VIB-3143) (#1581)
- Runner classifies CLOB 4xx rejections as non-retryable (VIB-3141) (#1588)
- Compound V3 base asset validated on borrow/repay (#1620) (#1636)
- Native supply/borrow emits a wrap tx on Aave-compatible lending protocols (#1619) (#1633)
- Spark native repay: fail-fast + wrap tx (#1621) (#1634)
- Kamino: fail-fast on non-Solana dispatch (#1622) (#1635)
- Fluid `trade_size_usd` reduced to stay within pool capacity (VIB-2844) (#1557)
- `HotCache` deep-copies on get/set to stop mutable state leak (VIB-3155) (#1569)
- Polymarket CLOB order submission works end-to-end (VIB-3012, VIB-3013, VIB-3014, VIB-3015) (#1537)
- Polymarket CLOB pre-flight validations run at dry-run (VIB-3140) (#1590)
- Polymarket passes `protocol="polymarket"` to resolve USDC.e on Polygon (VIB-3219) (#1609)
- Polymarket `prediction_monitor` snaps off-tick SELL `min_price` to the market tick (VIB-3217) (#1610)
- Polymarket `Position` backfills `market_id`/`token_id` from `conditionId` (SDK-P2-02) (#1605)
- Polymarket distinguishes accept from fill in CLOB execution (VIB-3218) (#1611)
- Polymarket preserves maker/taker ratio and defaults to limit when `max_price` is set (#1562)
- Backtesting populates `result.position_id` in `on_intent_executed` (VIB-2916, VIB-2918) (#1556)
- Gateway CLI advertises `ALMANAK_GATEWAY_AUTH_TOKEN` env var (VIB-2920) (#1540)
- Three regression bugs: anvil auth, Camelot, Fluid (VIB-3032, VIB-1636, VIB-2822) (#1539)
- Intent tests isolate modules via fork pristine revert (VIB-3059) (#1555)
- Smoke test pre-funds the `aerodrome_lp` wallet on Base (VIB-3057) (#1554)
- Sonic Silo V2 edge strategies excluded from nightly (blocked on connector) (VIB-2850, VIB-2851) (#1558)

## [2.13.0] - 2026-04-17

### Added
- PancakeSwap Perps connector on BSC (ApolloX) with 4-layer intent tests and demo strategy (#1506)
- PancakeSwap delta-neutral dynamic LP strategy (PCS V3 + PCS Perps hedge on BSC) (#1511)
- Morpho Blue on Monad with chain-specific deployment and 2 top-TVL markets (#1526)
- Morpho Blue on Polygon with 3 markets and WPOL/POL token aliases (#1529)
- Token resolution UX overhaul: 2500-token pre-populated JSON registry, negative cache, `almanak ax resolve` CLI (#1525)
- DexScreener fallback for EVM symbol resolution (#1530)
- Gateway address-based pricing for unknown tokens (#1532)
- 5 new demo strategies: Silo V2, Joe Lend, Euler V2, Lido staker, GMX V2 perp lifecycle (#1520)
- Top-level `chain` field in scaffold and demo config.json (#1508)

### Changed
- Remove deprecated TokenRegistry; use `get_token_resolver()` (#1495)

### Fixed
- Teardown production readiness: unified approval channel, hardened persistence, fail-closed safety (#1521)
- 0G Chain: Jaine DEX addresses, Gimo StakePool, W0G wrap mapping (#1528, #1502, #1522)
- Morpho Blue Arbitrum address correction (#1527)
- Runner lifecycle state reset after error recovery (#1519)
- Gateway auth token standardization (#1517, #1518)
- Silo V2 inverted collateral type enum (#1504)
- Address-only strategy hardening (#1505)
- PCS Perps Aster rebrand + over-hedge safety (#1514)
- LP dashboard template key alignment (#1516)
- MarketService reinitialization on chain info (#1512)
- Strategy loader skips abstract base classes (#1499)
- SQLite-only assertion on ensure_schema() (#1531)

## [2.12.0] - 2026-04-14

### Added
- Euler V2 lending connector on Avalanche and Ethereum with multi-chain adapter (#1409, #1453, #1424)
- Silo V2 isolated lending connector on Avalanche (#1407)
- Joe Lend (Banker Joe) lending connector on Avalanche (#1404)
- BenQi lending connector for Avalanche (#1402, #1396)
- Gimo Finance liquid staking connector for 0G Chain (#1456)
- 0G Chain (Zero Gravity) integration - enums, gas constants, RPC, token registry, demo strategies (#1456)
- X-Layer intent tests and documentation (#1416)
- Yield-aware paper trading with persistent fork, YieldPoker, and PnLBreakdown (#1428)
- Paper trading teardown - close positions on shutdown (#1441)
- Dashboard Accounting Phases 1-4 - rich accounting data, identity model, equity curve, traceability (#1457, #1463, #1482)
- OKX OnchainOS portfolio provider with Balance and DeFi APIs (#1410)
- `token_funding` field in strategy config.json for declarative Anvil wallet funding (#1406)
- Agentic cost optimization - tool catalog filtering, token telemetry, HuggingFace guard (#1465)
- 9 new demo strategies: Morpho+UniV3, Morpho+Enso, Aave+PancakeSwap, Aerodrome+Aave, Compound+Velodrome carry trades, Compound V3 on Polygon, Joe Lend lifecycle, Aave V3 debt probe, BenQi lifecycle (#1468, #1395, #1393, #1367, #1365, #1429, #1433, #1417, #1396)
- Chain registry and receipt parser registry completeness guard tests (#1446, #1452)

### Changed
- Upgrade Moralis to v2.2 endpoints for prices and DeFi positions (#1412)
- Update Meteora DLMM API to new datapi.meteora.ag endpoint (#1390)
- Bump almanak-code to v1.0.4 (#1476)
- V4 demo strategies default to Base instead of Ethereum (#1437)

### Fixed
- Fix asyncio UnboundLocalError in run.py that blocked `--once` runs (#1473)
- Gate gateway readiness on warmup completion (#1438)
- Pre-warm price cache before decide() to prevent cold-fork timeouts (#1388)
- Resolve amount='all' for withdraw/repay intents across all lending protocols (#1380)
- Retry on HTTP 429 in BaseIntegration instead of failing immediately (#1415)
- Prevent Compound V3 market IDs from leaking into token resolver (#1442)
- Compound V3 collateral withdraw_all uint128 overflow (#1363)
- Fix compiler repay_full bug for native AVAX (#1433)
- Paper trading balance-delta accounting and chain-ID integrity (#1382)
- Avalanche connector improvements - 17 issues across 4 lending adapters (#1414)
- Pendle pre-swap balance check, market coverage, and auto-detection (#1379)
- Stale Orca pool 404 and Anvil shutdown race (#1377)
- Add Ethereum token CoinGecko ID mappings for CVX, CRV, COMP, etc. (#1487)
- Verify Linea token decimals and storage slots (#1447)
- Anvil funding - WETH deposit() via wrapper, USDC whale impersonation, Sonic WETH (#1389)
- Make force_action one-shot across 12+ incubating strategies (#1485, #1484)
- 20+ strategy config and timeout fixes (#1400, #1399, #1392, #1479, #1486, #1480, #1478, #1466, #1430, #1431, #1467, #1474, #1481)

### Removed
- Remove uniswap_v3_swap_bsc strategy - no V3 liquidity on BSC (#1436)

### Security
- Bump cryptography from 46.0.6 to 46.0.7 (#1397)
- Remove leaked artifact, harden public mirror syncignore (#1422)

## [2.11.1] - 2026-04-07

### Changed
- Allow `almanak strat new -o .` in directories containing only dotfiles (#1374)
- Bump almanak-code to v0.2.13 (#1362)

### Fixed
- Fix nightly probe bugs: stranded LP position, phase corruption, missing pre-flight balance checks (#1373)
- Default balancer_flash_arb demo strategy to HOLD instead of swap to eliminate spurious nightly failures (#1370)
- Resolve copy replay fixture path relative to test file for pytest-xdist compatibility (#1375)
- Update allowed chains in gateway troubleshooting docs (add xlayer, sync translations)
- Update T&Cs link and add Spanish version in docs footer

## [2.11.0] - 2026-04-06

### Added
- Radiant V2 lending connector (#1334)
- Paper trading bootstrap with decide() dry-run inference (#1355, #1297)
- Paper-local token override registry for paper trading (#1356)
- Paper Trading Dashboard integration (#1342)
- Dashboard Phase 3 - ledger, data client, export, PM integration (#1327)
- Structured forensic events with cycle_id correlation (#1319)
- SavePortfolioMetrics/GetPortfolioMetrics gRPC endpoints (#1354)
- Multi-provider portfolio valuation with circuit-breaker failover (#1339)
- Gateway Zerion portfolio integration foundation (#1305)
- Portfolio valuation reconciliation and snapshot metadata (#1306)
- Dynamic Binance token resolution for price oracles (#1346)
- Dynamic token resolution - Jupiter/CoinGecko fallback + Solana guard (#1293)
- Pre-warm gateway price cache on startup (#1349)
- Orca SOL/USDC LP Anvil fork support + sdk.py IDL fixes (#1296)
- Aave V3 lending demo + compiler tests on Sonic (#1315)
- Spark Protocol wstETH/DAI full lending lifecycle (#1329)
- Auto-expand teardown complements in @almanak_strategy decorator (#1309)
- PnL backtest regression tests for uniswap_rsi on Arbitrum (#1323)
- Compound V3 compiler tests for Optimism and Polygon (#1308)
- Unit tests for ADX, OBV, CCI, Ichimoku calculators (#1299)
- 12 new demo strategies across 7 chains covering carry trades, lending, LP lifecycle, and yield stacks

### Changed
- Codebase hygiene Phase 1+2 - split 9 oversized files into sub-modules (#1341, #1352)
- Auto-expand teardown complement intent types in permission generation (#1270)
- Bump Almanak Code to v0.2.12

### Fixed
- Recompute LP amounts from on-chain sqrtPriceX96 to prevent price slippage reverts (#1288)
- Route Compound V3 SUPPLY to supply_collateral() for collateral tokens (#1310)
- Route Compound V3 WITHDRAW to withdraw_collateral() for collateral tokens (#1311)
- Resolve amount='all' in BridgeIntent compilation by querying from_chain balance (#1275)
- Morpho Blue repay_full=True uses correct RPC on Anvil fork (#1278)
- Persist strategy state after each successful teardown intent (#1279)
- Capture portfolio snapshot on all iteration outcomes (#1324)
- Add certifi SSL context to RpcService aiohttp session (#1269)
- Anvil port race condition with retry logic (#1321)
- Add stETH/rETH to token registry + guard balance() silent-zero (#1287)
- Paper trading bootstrapping - checksum addresses, preserve symbol case (#1295)
- Inject simulated_balances from config in dry-run mode (#1291)
- Auto-default strat new to strategies/incubating/ from SDK root (#1290)
- Skip gateway timeout in cosmetic token symbol lookups (#1298)
- Suppress false-positive amount chaining warnings (#1307)
- Add gas buffer to simulator state-setup execution (#1333)
- Compound V3 collateral routing, gateway price pre-warm fixes (#1335)
- Throttle dashboard event spam, harden crash resilience (#1338)
- Bump Zerion cache TTL from 60s to 300s (#1336)
- Restore PancakeSwap Aave carry BSC strategy deleted by #1332 (#1361)
- Authoritative registry files use BASE-WINS conflict resolution (#1268)

### Security
- Bump cryptography from 46.0.5 to 46.0.6 (#1161)

## [2.10.0] - 2026-04-02

### Added
- X-Layer chain support with Aave V3.6 carry and LP rebalance demo strategies (#1252)
- Monad production activation with demo strategies and infra (#1248)
- Lido stETH + Aave V3 wstETH supply composition on Ethereum (#1228)
- Morpho Blue wstETH/USDC full lifecycle on Base (#1238, #1261)
- SushiSwap V3 + Aave V3 T2 composition on Arbitrum (#1256)
- Morpho Blue + Enso lifecycle on Base (#1255)
- BENQI leveraged loop + Enso swap + teardown on Avalanche (#1237)
- Compound V3 lending lifecycle execution on Polygon (#1232)
- Morpho Blue crisis scenario backtest on Ethereum (#1231)
- Paper trading tests for Compound V3 + Aerodrome composed strategy (#1235)
- LST/LRT token addresses for swETH, ankrETH, pufETH, CVX (#1241)
- Demo strategies included in nightly test suite with test-reporter prompt (#1244)

### Changed
- Bump almanak-code to v0.2.10 (#1249)

### Fixed
- Aave/Spark repay_full queries wallet balance instead of sending MAX_UINT256 (#1266)
- Apply certifi SSL context to all AsyncHTTPProvider instances (#1265)
- X-Layer RPC public fallback, SSL cert fix, correct USDT0 address (#1264)
- Compound V3 support added to Polygon rate monitor (#1259)
- Agni Finance fee tier 3000 to 500 for Mantle swap pools (#1258)
- TraderJoe V2 extract_swap_amounts uses actual token decimals (#1251)
- Intent.repay() optional amount when repay_full=True (#1250)
- Aerodrome LP_CLOSE permission discovery with static removeLiquidity hint (#1243, #1246)
- Strategy templates use symbolic pool format and provide both LP amounts (#1242)
- Pendle YT sell floor responds to TeardownManager slippage escalation (#1224)
- Anvil --no-gas-cap replaced with --block-base-fee-per-gas 0 for all versions (#1253)
- Batch Quick Win bug fixes: chain-specific WETH in diagnostics, intent state machine fail-fast, teardown retry on SKIPPED, Enso routing (#1236)
- Nightly test failures: anvil_funding configs, compound_v3 polygon, probe excludes (#1254, #1257)

## [2.9.0] - 2026-04-01

### Added
- `Intent.wrap()` and `Intent.unwrap()` factory methods for native token wrapping (#1196)
- Polish circuit breaker for strategy execution quality gates (#1229)
- Lido enricher exposes wstETH amount for `receive_wrapped=True` (#1230)
- GeckoTerminal DEX OHLCV fallback with gRPC proxy for deployed mode (#1112, #1200)
- Teardown intent introspection for automatic protocol discovery (#1193)
- Morpho Blue Arbitrum support with Uniswap V3 yield stack strategy (#1234)
- Curve StableSwap 3pool LP lifecycle strategy on Ethereum (#1220)
- Compound V3 + Enso leveraged swap strategy on Base (#1207)
- Compound V3 + Aerodrome yield farm strategy on Base (#1194)
- Morpho Blue + Uniswap V3 leveraged LP strategy on Ethereum (#1197)
- BENQI + Uniswap V3 leveraged swap teardown lifecycle on Avalanche (#1210)
- Aave V3 + PancakeSwap V3 teardown lifecycle on BSC (#1211)
- Compound V3 + Uniswap V3 teardown lifecycle on Arbitrum (#1212)
- Compound V3 + Aerodrome teardown lifecycle on Base (#1202)
- Enso swap lifecycle tests on Base (#1233)
- Velodrome V2 PnL backtest on Optimism (#1177)
- Almanak brand fonts and consistent footer for SDK docs site

### Changed
- SDK documentation URL updated from docs.almanak.co to sdk.docs.almanak.co
- Bump Almanak Code version to v0.2.9

### Fixed
- Halt strategy runner after teardown failure instead of continuing (#1111)
- Route Aerodrome pool address query through gateway RPC (#1227)
- Fix Pydantic intent cloning in teardown slippage escalation using `model_copy` (#1226)
- Pass `routing_strategy=router` in gRPC EnsoRouteRequest (#1225)
- Bump Stargate LayerZero fee estimates with route-aware values (#1223)
- Repair 5 broken demo strategies found by Anvil audit (#1216)
- Widen Enso slippage on Anvil forks to prevent safeRouteSingle reverts (#1206)
- V4 receipt parser `extract_position_id` fallback and logging (#1204)
- Pass config.json to teardown introspection in permissions CLI (#1203)
- Route portfolio snapshots through gateway gRPC instead of SQLite fallback (#1205)
- Resolve CoinGecko prices by contract address via registry lookup (#1199)
- Thread `data_granularity` config into all indicator methods (#1198)
- Pendle YT teardown auto-mode slippage escalation (#1195)
- Raise V4 LP estimated-price slippage buffer to 30% (#1192)

## [2.8.1] - 2026-03-30

### Added
- Aave V3 parameter sweep lending demo on Polygon (#1152)
- PancakeSwap V3 swap lifecycle demo on Base (#1171)
- Uniswap V3 swap lifecycle demos on Optimism (#1172) and Base (#1176)
- Crisis scenario backtest for Compound V3 lending on Polygon (#1173)

### Fixed
- Prevent unexpected `chains` kwarg in strategy `__init__` (#1178)
- Add S and WS (Wrapped Sonic) tokens to default registry (#1180)
- Add WAL auto-checkpoint to prevent SQLite state database bloat (#1184)

## [2.8.0] - 2026-03-30

### Added
- All 75 demo strategies now accessible via `almanak strat demo` - migrated 59 strategies from `strategies/demo/` into the packaged `almanak/demo_strategies/` directory (#1188)
- Stub README.md generated for all demo strategies with Quick Start instructions (#1188)

### Changed
- Demo strategies now live exclusively in `almanak/demo_strategies/` - the `strategies/demo/` directory has been removed (#1188)
- Updated all path references across framework CLI, tests, blueprints, and agent docs (#1188)

### Fixed
- V4 LP_OPEN on-chain sqrtPrice query, approve gas estimation, and slippage fallback (#1187)
- EIP-55 checksum for 3 addresses in demo strategies (#1188)
- `STRATEGY_METADATA` attribute access in strategy `__main__` blocks (#1188)
- `force_action="supply"` in aave_borrow no longer blocked by unavailable price oracle (#1188)
- `default_chain` indentation in 30 strategy decorators (#1188)

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
- **Multi-language documentation** site at sdk.docs.almanak.co (English, Mandarin, French, Spanish)
- **AI agent skills**: Strategy builder skill for Claude Code, Codex, Cursor, Copilot, and 6 more platforms via `almanak agent install`
- **Non-custodial Safe design**: Fine-grained permission controls through Zodiac Roles Modifier, user maintains full control of funds
- **Three-tier state management**: Automatic HOT/WARM/COLD persistence for strategy state
- **Production services**: Alerting (Slack/Telegram), stuck detection, emergency management, canary deployments
