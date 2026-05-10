# L1 Offline Goldens — Uniswap V3 on Arbitrum

**Owner**: VIB-4194 (T08 of epic VIB-4185 Multi-Position Tracking)
**Consumers**: registry tests (T11/T12) and accounting tests. *One golden, two consumers* per
PRD §Pyramid Mapping L1 (`docs/internal/prds/multi-position-tracking.md` line 573).
**Status**: immutable contracts. A parser change that requires a golden update is an
intentional, reviewed event — never a silent rebase. See PRD line 705 for the
"goldens are immutable contracts" rule.

## What's here

Three fixture sets covering the three Tier-1 UniV3 intent types on Arbitrum:

| Fixture | Intent | Real source |
|---|---|---|
| `swap/` | `SWAP` | Anvil-fork Arbitrum tx `0x9481aa999495ed7ca79edcfc2b7ef1a1a0008e8cdd4e72df5068cc38a9de0bc6` (UniV3 USDC → USDT, block 459393901). Run captured 2026-05-04 in `tests/reports/anvil-sweep-2026-05-05/fluid_swap_arb/`. |
| `lp_open/` | `LP_OPEN` | Anvil-fork Arbitrum tx `0x7620d5ed03d0d655247f357d207c9b7fd25af88513c6393495c454cab3efc0df` (UniV3 WETH/USDC 0.05% mint, NFT tokenId **5467895**, block 459405352). Run captured 2026-05-04 in `tests/reports/anvil-sweep-2026-05-05/uniswap_lp/`. |
| `lp_close/` | `LP_CLOSE` | Anvil-fork Arbitrum tx `0xea2bc0637faf4b987a5df37069ea2bb02b7305a6f533f58ace3597c7173830a2` (UniV3 LP_CLOSE for tokenId **5467895**, block 459405353). Same run as `lp_open/` — the LP_OPEN/LP_CLOSE pair shares one NFT, so `physical_identity_hash` is byte-identical across the two registry rows. This is the load-bearing property: T11/T12 use the equality to test position lifecycle matching. |

**Mainnet cross-check**: a separate real Arbitrum mainnet UniV3 LP_OPEN (block 430339188,
tx `0x96697458d5f3646fc1d5a829d04b0ac42182dbb8d0fbe210565fe06861717dc9`, NFT tokenId 5296102,
WETH/USDC 0.05%) is documented at `tests/reports/uniswap_lp_mainnet_report.md`. The
shapes in `lp_open/receipt.json` match that mainnet receipt's structure verbatim;
the Anvil-fork tx hash is used because it was produced under the same Anvil-fork
flow that T11/T12's registry consumers will exercise.

## Files per fixture set

Each of `swap/`, `lp_open/`, `lp_close/` contains six files:

| File | Shape | Producer | Consumer |
|---|---|---|---|
| `receipt.json` | EVM transaction-receipt dict — top-level keys `transactionHash`, `blockNumber`, `logs[]`. Each log has `address`, `topics[]`, `data`. | UniV3 receipt parser input | Both |
| `pre_state.json` | Pre-execution wallet/pool snapshot relevant to the operation. | Used by L2 enricher / accounting handler tests. | Accounting |
| `oracle_snapshot.json` | Token prices at the operation's block. | `price_inputs_json` for the ledger row. | Accounting |
| `expected_ledger_row.json` | The `LedgerEntry` shape post `build_ledger_entry()` — ready to insert into `transaction_ledger`. | T08 hand-author. | Both |
| `expected_registry_row.json` | The 16-column `position_registry` row per PRD §Registry Data Shape. SWAP carries an explicit "no registry row" sentinel because SWAP is not a position-establishing intent. | T08 hand-author. | Registry (T11 / T12) |
| `expected_accounting_event.json` | Typed accounting-event payload with `position_reference` per PRD §Position Reference Shape. SWAP omits `position_reference` (no position). | T08 hand-author. | Accounting (T26+ enricher tests) |

## Identity invariants

The hand-authored expected rows MUST satisfy these invariants — the loader test at
`tests/unit/multi_position_tracking/test_l1_goldens_univ3.py` enforces them:

1. **`physical_identity_hash` is receipt-derivable.** Per PRD §Hard Gates Gate 1 +
   `docs/internal/qa/parser-coverage-audit-tier1-20260508.md`, the hash inputs for UniV3
   are `(chain, nft_manager_addr, token_id)`. `nft_manager_addr` is the parser's
   chain-keyed config constant *and* is the emitter address of the
   `IncreaseLiquidity` log; `token_id` is `IncreaseLiquidity.topics[1]` decoded as
   uint256. The loader recomputes the hash from the receipt and asserts equality.
2. **LP_OPEN.physical_identity_hash == LP_CLOSE.physical_identity_hash.** Same NFT
   (5467895). The pair shares one identity so T11/T12 can test "open → close" lifecycle
   matching by hash equality. Failing this would silently corrupt every downstream
   identity-matching test.
3. **`primitive` and `accounting_category` from canonical taxonomy.** PRD §Registry
   Data Shape lines 156-157: registry rows MUST stamp `Primitive.LP.value` and
   `AccountingCategory.LP.value` (both `'lp'`), sourced from `record_for('LP_OPEN')`.
4. **`matching_policy_version == MATCHING_POLICY_VERSIONS[Primitive.LP]` (== 3).**
   Per PRD line 159 — per-primitive, never global. The literal `3` in the goldens
   MUST agree with the runtime constant.
5. **`grouping_policy_version`** is `'univ3_lp@v1'` — the canonical version string
   per PRD §Registry Data Shape (line 126 example).
6. **`position_reference.source = 'receipt'`** (Day-1 mode per PRD line 515 —
   pre-cutover, parser-derived). The shape is fixed forward-compatibly; flipping to
   `'registry'` is a future PR's `source`-only change.

## Hash computation

The loader test recomputes `physical_identity_hash` from receipt facts as:

```python
import hashlib
nft_manager = "0xc36442b4a4522e871399cd717abdd847ab11fe88"  # arbitrum
token_id = int(receipt["logs"][i]["topics"][1], 16)         # IncreaseLiquidity.topics[1]
chain = "arbitrum"
seed = f"{chain}:{nft_manager.lower()}:{token_id}"
expected_hash = "0x" + hashlib.sha256(seed.encode()).hexdigest()
```

The hash is deterministic SHA-256; running the recompute on the same receipt MUST
produce the byte-identical hash stored in the goldens. This is *the* property T11/T12
lean on. The full 64-char hex is used (vs the 12-char deployment-id truncation) to
keep the registry's primary key collision-resistant under realistic position counts.

## Why no live execution / Anvil round-trip in this PR

T08 is L1 (offline goldens) only. The Anvil round-trip is L5 (`docs/internal/prds/
multi-position-tracking.md` line 577) and lives in T12 (VIB-4198). This PR's
contribution is the static shape contract; the runtime contract sits in T12.

## Related tickets

- **T05 (VIB-4190)**: `position_registry` SQLite schema. Shipped.
- **T11 (VIB-4197)**: atomic `save_ledger_and_registry` primitive + `RegistryRow`
  dataclass. Shipped — these goldens construct `RegistryRow(**json_dict)` to
  validate column names match.
- **T06 (VIB-4192)**: `Intent.registry_handle` reserved field. Shipped.
- **T10 (VIB-4196)**: production `position_reference` shape on `accounting_events`.
  Lands independently. These goldens specify the shape T10 must emit.
- **T12 (VIB-4198)**: first cutover (UniV3 LP) — consumes these goldens for the L2
  contract test ("ledger row + registry row land atomically and match goldens").
