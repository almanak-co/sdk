# Handoff: Solana Chain Foundation (VIB-73 + VIB-74)

**Date**: 2026-02-28
**Branch**: `chain/solana` (from `main` at `864cb4ce`)
**Worktree**: `/Users/nick/Documents/Almanak/src/almanak-sdk/.claude/worktrees/solana`

---

## Context

### Epic: VIB-68 — Solana Chain Support + Jupiter DEX Integration

The Almanak SDK is adding Solana as a first-class chain. This is a **Moonshot** (14-18 engineering weeks MVP). The codebase is deeply EVM-oriented — Solana is a new execution substrate, not just "another chain."

**PRD** (source of truth): Attached to VIB-68 in Linear. Generated from 10-round AI discussion (Gemini/Codex/Claude) on 2026-02-26. Title: "PRD: Solana Chain Support + Jupiter DEX Integration." Fetch via `get_issue VIB-68` or `get_attachment e947fc75-858c-497c-a704-8cee0e31174e`.

### Key Architecture Decisions (from PRD)

- **Strategy surface unchanged**: `IntentStrategy.decide()` is chain-agnostic. Strategy authors write zero Solana-specific code.
- **SolanaExecutionPlanner**: Single owner of all Solana mechanics (ATA preflight, LUT resolution, CU simulation, JIT blockhash, Ed25519 signing, signature confirmation). Lives in Gateway.
- **Jupiter for Phase 1 DEX**: API-assisted aggregator (https://api.jup.ag), like Enso for EVM. No AMM math.
- **Balance-delta receipt parsing**: `pre/postTokenBalances` normalized for WSOL wrap/unwrap and ATA rent noise. Skip Anchor IDL decoding for MVP.
- **CoinGecko price fallback**: MVP-acceptable for backtesting. Pyth native integration gates mainnet.
- **No `if chain == solana` sprinkled everywhere**: Clean chain-family abstraction with routing at 6 chokepoints.
- **SolanaForkManager**: Uses `solana-test-validator` with cloned accounts. MVP-blocking. Pyth publish_time advancement required to prevent stale-price rejections.

---

## What Was Done This Session

### 1. Ticket Restructuring (complete)

All tickets under VIB-68 were updated to match the PRD:

| Ticket | Title | Status |
|--------|-------|--------|
| VIB-68 | Epic: Solana Chain Support + Jupiter DEX Integration | Updated (Jupiter not Raydium, correct child IDs) |
| VIB-73 | Design + RFC — chain-family abstraction and interfaces | Updated (14 specific deliverables) |
| VIB-74 | Chain Foundation — enums, validation, RPC, token resolver | Updated (added ChainFamily, ATAPolicy, etc.) |
| VIB-75 | Execution Engine — SolanaExecutionPlanner | Updated (full PRD spec) |
| VIB-76 | Jupiter Connector + Intent Compilation + Receipt Parsing | Renamed from Raydium, added receipt + price |
| VIB-77 | Hardening, test stabilization, docs, demo strategy | Updated done criteria from PRD |
| **VIB-366** | SolanaForkManager — Local Testing Environment | **NEW** (was missing, MVP-blocking) |
| **VIB-367** | Program Integrity Monitoring | **NEW** (Phase 1.5, not MVP-blocking) |

### 2. Dependencies Wired (complete)

```
VIB-73 (Design) → VIB-74 (Foundation) → VIB-75 (Execution) → VIB-76 (Jupiter) → VIB-77 (Hardening)
                         ↘ VIB-366 (ForkManager) ↗
VIB-367 (Program Integrity) — standalone, Phase 1.5
```

### 3. Worktree + Branch Created (complete)

- Worktree at `.claude/worktrees/solana`
- Branch `chain/solana` from `main` at `864cb4ce`

---

## What Needs To Happen Now

### Task 1: VIB-73 — Design + RFC

**Deliverable**: `docs/internal/solana-design-rfc.md`

Write a design doc with interface signatures for all 14 items:

1. **ChainFamily discriminator**: How `Chain.SOLANA` routes to Solana-specific strategies at each of the 6 EVM chokepoints
2. **ChainExecutionStrategy interface**: Abstract base for EVM vs Solana execution (nonce/blockhash, gas/CU, signing, confirmation)
3. **SolanaExecutionPlanner interface**: ATA preflight, LUT resolution, CU simulation, JIT blockhash, ExpiryPolicy, signature confirmation
4. **ATAPolicy enum**: `AUTO_CREATE` vs `REQUIRE_EXISTING`
5. **AtomicityRequirement enum**: `ATOMIC` / `BEST_EFFORT` / `SEQUENTIAL_REQUIRED` (on IntentSequence)
6. **CommitmentLevel enum**: `processed` / `confirmed` / `finalized` (mirrors Solana RPC)
7. **SolanaExecutionHints model**: ata_policy, allow_ata_creation, explicit_accounts
8. **Address validation dispatch**: `validate_address_for_chain(addr, chain)` — hex vs base58
9. **Transaction model extension**: How `UnsignedTransaction` accommodates Solana (accounts, program_id) without breaking EVM
10. **Token resolver Solana adapter**: Base58 mint support, WSOL in WRAPPED_NATIVE
11. **Intent compiler routing**: `ProtocolAdapterFactory` per chain family
12. **Jupiter connector API surface**: HTTP API wrapper, instruction extraction
13. **SolanaForkManager interface**: start, fund_test_wallet, warp_price, advance_clock
14. **Receipt parsing approach**: Balance-delta (Layer 1) vs Anchor IDL (Layer 2) strategy

### Task 2: VIB-74 — Chain Foundation Implementation

After the design doc, implement the actual code:

**Enums + Config** (`almanak/core/enums.py`):
- `Chain.SOLANA` added to Chain enum
- `ChainFamily` enum: `EVM` / `SOLANA` with `CHAIN_FAMILY_MAP: dict[Chain, ChainFamily]`
- `CommitmentLevel` enum: `PROCESSED` / `CONFIRMED` / `FINALIZED`
- `ATAPolicy` enum: `AUTO_CREATE` / `REQUIRE_EXISTING`
- `AtomicityRequirement` enum: `ATOMIC` / `BEST_EFFORT` / `SEQUENTIAL_REQUIRED`

**New model** (location TBD by design doc — likely `almanak/core/models/` or `almanak/framework/execution/`):
- `SolanaExecutionHints` dataclass/model

**Address Validation** (`almanak/gateway/validation.py`):
- `validate_address_for_chain(address, chain)` dispatcher
- EVM path: existing `^0x[a-fA-F0-9]{40}$`
- Solana path: base58 validation (32-44 chars, base58 alphabet)
- Update callers as needed

**RPC Provider** (`almanak/gateway/utils/rpc_provider.py`):
- Add Solana to `ALCHEMY_CHAIN_KEYS`
- Add Solana public RPCs (mainnet-beta + devnet)

**Token Resolver**:
- `almanak/framework/data/tokens/defaults.py`: Solana mint addresses (SOL, USDC, USDT, JUP) in base58
- `almanak/framework/data/tokens/resolver.py`: Base58 address pattern support in `_is_address()`
- `WRAPPED_NATIVE["solana"]` = `"So11111111111111111111111111111111111111112"` (WSOL mint)

**Constraints**:
- All existing EVM tests must remain green
- Run `make lint` before finishing
- Run `make test` to verify no regressions

---

## Key Files To Read First

| File | What To Look For |
|------|-----------------|
| `almanak/core/enums.py` | `Chain` enum, `CHAIN_IDS`, `SUPPORTED_PROTOCOLS`, `Network` enum |
| `almanak/gateway/validation.py` | `ADDRESS_PATTERN`, `validate_address()`, `ALLOWED_CHAINS` |
| `almanak/gateway/utils/rpc_provider.py` | `ALCHEMY_CHAIN_KEYS`, `PUBLIC_RPC_URLS`, URL construction |
| `almanak/framework/data/tokens/resolver.py` | `TokenResolver`, `ADDRESS_PATTERN`, `_is_address()`, `resolve()` |
| `almanak/framework/data/tokens/defaults.py` | `WRAPPED_NATIVE`, `STATIC_TOKEN_REGISTRY`, token format |
| `almanak/framework/execution/orchestrator.py` | EVM execution pipeline (nonce, gas, signing) |
| `almanak/framework/intents/compiler.py` | Intent→ActionBundle, protocol adapter routing |
| `almanak/core/models/transaction.py` | `UnsignedTransaction` fields (to, data, value, gas, nonce) |

---

## 6 EVM Chokepoints Identified

These are the specific places where EVM assumptions are hardcoded and need chain-family routing:

| # | Chokepoint | File | EVM Assumption |
|---|-----------|------|----------------|
| 1 | Address validation | `gateway/validation.py` | `^0x[a-fA-F0-9]{40}$` |
| 2 | Execution pipeline | `framework/execution/orchestrator.py` | Nonce, gas, gwei, secp256k1 |
| 3 | Transaction model | `core/models/transaction.py` | `gas`, `nonce`, hex calldata |
| 4 | Token resolver | `framework/data/tokens/resolver.py` | Hex address pattern |
| 5 | Intent compiler | `framework/intents/compiler.py` | EVM calldata, router addresses |
| 6 | RPC provider | `gateway/utils/rpc_provider.py` | Alchemy URL patterns (config-only) |

---

## Solana-Specific Technical Details (from PRD)

- **Addresses**: Base58-encoded, 32-44 chars, no `0x` prefix
- **WSOL mint**: `So11111111111111111111111111111111111111112`
- **USDC mint (Solana)**: `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`
- **USDT mint (Solana)**: `Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB`
- **JUP mint**: `JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN`
- **Jupiter API**: `https://api.jup.ag` (swap routing, quotes)
- **Jupiter Router program**: Address to be confirmed during implementation
- **Transaction model**: Versioned Transactions (V0), Ed25519 signatures, ~150 slot expiry (~60s)
- **Confirmation**: `getSignatureStatuses` (not `eth_getTransactionReceipt`)
- **Commitment levels**: processed → confirmed → finalized
- **ATA**: Associated Token Accounts must be created before receiving SPL tokens
- **CU**: Compute Units replace gas; simulateTransaction gives estimate; 1.2x buffer
- **Local testing**: `solana-test-validator` with `--clone` for account cloning
- **Pyth**: Account-based push oracle; publish_time must be advanced during clock warp to avoid staleness rejection (60s window)
