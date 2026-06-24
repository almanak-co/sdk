# Benqi Looping (Demo)

A **leveraged-long-AVAX** strategy on BENQI (a Compound V2 fork on Avalanche) —
the quant counterpart to the `benqi_lending_lifecycle` tutorial, and the
BENQI/Compound-V2 sibling of `morpho_looping`.

It builds a recursive cross-asset leverage position (long AVAX, funded by USDC
debt), holds it under a health-factor watch, and unwinds it safely on HF-danger
or teardown:

```text
BUILD  (target_loops rounds):
   supply AVAX → borrow USDC → swap USDC→WAVAX → unwrap WAVAX→AVAX → re-supply
HOLD   monitor the health factor every tick (real, price-driven)
UNWIND on HF ≤ hf_danger (AVAX fell) OR teardown:
   withdraw AVAX → wrap AVAX→WAVAX → swap WAVAX→USDC → repay USDC  (staircase)
   → final withdraw-all of residual AVAX
```

Because BENQI's AVAX market is **native** (`qiAVAX.mint()` payable) while DEXes
trade **wrapped** WAVAX, each loop carries a wrap/unwrap leg. AVAX is also the
gas token, so the strategy supplies/wraps **specific tracked amounts** and
always leaves `gas_reserve` untouched (never `"all"` on a native leg).

The collateral leg is volatile, so the health factor genuinely moves and the
HF-danger unwind is a real risk control — not a dormant branch. (The cross-asset
loop where collateral == borrow and no swap is needed is *not* this demo; that
single-asset shape is simpler but has no HF dynamics.)

## Chain

avalanche

## Configuration (`config.json`)

| Key | Meaning |
|---|---|
| `collateral_token` / `borrow_token` | `AVAX` (native collateral) / `USDC` (debt). |
| `wrapped_native` | DEX-side wrapped token (`WAVAX`) for the swap legs. |
| `initial_collateral` | First AVAX supply (token units). |
| `target_loops` | Number of borrow→swap→re-supply rounds (leverage rounds). |
| `target_ltv` | Borrow this fraction of each supply's USD value; must be `< collateral_factor`. |
| `collateral_factor` | BENQI Comptroller collateral factor for AVAX (sets the HF). |
| `hf_danger` | Unwind when the health factor drops to this level. |
| `hf_unwind_floor` | Per-round post-withdraw HF floor during the unwind staircase. |
| `swap_slippage` | Max slippage on each DEX swap. |
| `gas_reserve` | Native AVAX held back for gas — never supplied or wrapped. |

## Quick Start

```bash
almanak strat demo --name benqi_looping
cd benqi_looping
uv run almanak strat run --network anvil --interval 15
```
