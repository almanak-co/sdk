# Enums

Core enumeration types used throughout the SDK.

!!! note "Chains are identified by name strings"
    There is no `Chain` enum. Chains are identified by canonical lowercase
    name strings (`"ethereum"`, `"arbitrum"`, `"solana"`, ...). Resolve
    names, aliases, and CAIP-2 ids through
    `almanak.core.chains.ChainRegistry` — e.g.
    `ChainRegistry.resolve("bnb").name == "bsc"`,
    `ChainRegistry.get("arbitrum").chain_id == 42161`.

## Network

::: almanak.core.enums.Network
    options:
      show_root_heading: true
      members_order: source

## ActionType

::: almanak.core.enums.ActionType
    options:
      show_root_heading: true
      members_order: source

## ExecutionStatus

::: almanak.core.enums.ExecutionStatus
    options:
      show_root_heading: true
      members_order: source

## SwapSide

::: almanak.core.enums.SwapSide
    options:
      show_root_heading: true
      members_order: source

## TransactionType

::: almanak.core.enums.TransactionType
    options:
      show_root_heading: true
      members_order: source
