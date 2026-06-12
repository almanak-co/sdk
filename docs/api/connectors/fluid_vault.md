# Fluid Vault (NFT-CDP)

Connector for Fluid's vault borrow surface (`protocol="fluid_vault"`):
NFT-CDP lending positions driven by a single signed-delta `operate()`
entrypoint per type-1 vault on Arbitrum and Base. `market_id` is the vault
address and is required on every intent; the fToken lending surface
(`protocol="fluid"`) is documented under [Fluid](fluid.md).

::: almanak.connectors.fluid_vault
    options:
      show_root_heading: true
      members_order: source

## Implementation modules

The implementation lives in the `fluid` package (one codebase, two
manifests):

::: almanak.connectors.fluid.vault_sdk
    options:
      show_root_heading: true
      members_order: source

::: almanak.connectors.fluid.vault_compiler
    options:
      show_root_heading: true
      members_order: source
