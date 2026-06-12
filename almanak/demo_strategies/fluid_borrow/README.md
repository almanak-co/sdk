# Fluid Vault Borrow Demo (NFT-CDP)

Tutorial strategy for Fluid's vault borrow surface (`protocol="fluid_vault"`,
VIB-5031) on **Arbitrum vault id 1** (native-ETH collateral → USDC debt,
`0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C`).

## What it shows

- **Atomic open**: ONE `BorrowIntent` carrying both a collateral amount and a
  borrow amount compiles to a single on-chain call —
  `operate(nftId=0, +collateral, +debt, wallet)` — which mints the position
  NFT (ERC-721 on the VaultFactory) and draws the debt in one transaction.
- **`market_id` is the vault address** and is required on every `fluid_vault`
  intent (isolated markets, Morpho-style; the fToken surface
  `protocol="fluid"` instead *rejects* `market_id`).
- **Native collateral**: vault id 1 takes raw ETH as `msg.value` — no WETH
  wrap, no approve on the collateral leg.
- **Safe repays**: the demo's `repay` action is a fixed PARTIAL repay
  (`repay_amount`, default 50 USDC), pre-flighted against the live debt
  (Fluid's over-repay revert `Vault__ExcessDebtPayback` / 31015 is
  unreachable from a compile that passed). Full closes are not part of
  this demo — they live in the E2E strategy / teardown path, via
  `repay_full=True` (the protocol's int-min sentinel resolves the exact
  debt at execution time).

## Run it

```bash
# Atomic open (default):
python almanak/demo_strategies/fluid_borrow/run_anvil.py

# Forced single actions — NOTE: each invocation starts a FRESH fork (no
# state carries over between runs). `supply` mints a new position with the
# collateral; `repay` needs existing debt, so open a position first in the
# same interactive session (the script keeps Anvil alive until you exit):
python almanak/demo_strategies/fluid_borrow/run_anvil.py supply
python almanak/demo_strategies/fluid_borrow/run_anvil.py repay

# CI sidecar mode (fund + block, gateway started by the workflow):
python almanak/demo_strategies/fluid_borrow/run_anvil.py --skip-cli
```

Requires `ALMANAK_ARBITRUM_RPC_URL` in `.env` and Foundry (`anvil`/`cast`).
