# Enums

Core enumeration types used throughout the SDK.

!!! note "Chains are identified by name strings"
    There is no `Chain` enum. Chains are identified by canonical lowercase
    name strings (`"ethereum"`, `"arbitrum"`, `"solana"`, ...). Resolve
    names, aliases, and CAIP-2 ids through
    `almanak.core.chains.ChainRegistry` — e.g.
    `ChainRegistry.resolve("bnb").name == "bsc"`,
    `ChainRegistry.get("arbitrum").chain_id == 42161`.

::: almanak.core.enums.Network
    options:
      heading_level: 2
      members_order: source

::: almanak.core.enums.ActionType
    options:
      heading_level: 2
      members_order: source

::: almanak.core.enums.ExecutionStatus
    options:
      heading_level: 2
      members_order: source

::: almanak.core.enums.SwapSide
    options:
      heading_level: 2
      members_order: source

::: almanak.core.enums.TransactionType
    options:
      heading_level: 2
      members_order: source
