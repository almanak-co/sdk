# Fluid DEX LP (SmartLending)

Connector for Fluid's DEX LP surface (`protocol="fluid_dex_lp"`): fungible
ERC-20-share, two-token liquidity positions over Fluid DEX pools via the
SmartLending wrappers on Arbitrum. There is no NFT and no tick range — the
wrapper share balance is the position. Direct pool LP is whitelist-gated
on-chain, so the wrapper (the whitelisted supplier) is the only retail route;
the compiler pre-flights deposit-enabled and refuses a disabled pool at compile.
The fToken lending surface (`protocol="fluid"`) is documented under
[Fluid](fluid.md) and the vault borrow surface under
[Fluid Vault](fluid_vault.md).

::: almanak.connectors.fluid_dex_lp
    options:
      show_root_heading: true
      members_order: source

## Implementation modules

The implementation lives in the `fluid` package (one codebase, three
manifests):

::: almanak.connectors.fluid.smart_lending_sdk
    options:
      show_root_heading: true
      members_order: source

::: almanak.connectors.fluid.dex_lp_compiler
    options:
      show_root_heading: true
      members_order: source
