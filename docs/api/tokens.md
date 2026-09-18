# Token Resolution

Unified token resolution for addresses, decimals, and symbol lookups across all chains.

!!! warning "Symbol-based token references are deprecated"

    Token symbols are metadata, not stable asset identity — the same ticker
    resolves to a different contract on every chain and can be spoofed.
    Passing a bare symbol to `TokenResolver`, `MarketSnapshot`, or `Intent`
    construction emits a `SymbolTokenResolutionWarning` (a `FutureWarning`),
    once per external callsite.

    Symbols keep working for the remainder of the 2.x line and are **rejected
    in Almanak SDK 3.0.0** with `SymbolTokenResolutionError`. Prefer a
    chain-specific contract address or a
    [CAIP-19](https://chainagnostic.org/CAIPs/caip-19) asset identifier.

## Usage

```python
from almanak.framework.data.tokens import get_token_resolver

resolver = get_token_resolver()

# Preferred: resolve by address — stable asset identity
token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
print(token.symbol, token.decimals)  # USDC 6

# Preferred: resolve by CAIP-19 asset identifier
token = resolver.resolve_caip19("eip155:42161/erc20:0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

# Deprecated: resolve by symbol — warns today, raises in 3.0.0
token = resolver.resolve("USDC", "arbitrum")

# Convenience methods (also symbol-deprecated when given a bare symbol)
decimals = resolver.get_decimals("arbitrum", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
address = resolver.get_address("arbitrum", "USDC")

# For DEX swaps (auto-wraps native tokens: ETH->WETH, etc.)
token = resolver.resolve_for_swap("ETH", "arbitrum")
```

To check whether a value already carries address-based identity before handing
it to the SDK:

```python
from almanak.framework.data.tokens.deprecation import is_address_based_token_reference

is_address_based_token_reference("USDC", "arbitrum")  # False - deprecated symbol
is_address_based_token_reference("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")  # True
```

::: almanak.framework.data.tokens.get_token_resolver
    options:
      heading_level: 2

::: almanak.framework.data.tokens.TokenResolver
    options:
      heading_level: 2
      members_order: source

::: almanak.framework.data.tokens.ResolvedToken
    options:
      heading_level: 2

::: almanak.framework.data.tokens.BridgeType
    options:
      heading_level: 2

## Exceptions

::: almanak.framework.data.tokens.TokenResolutionError
    options:
      heading_level: 3

::: almanak.framework.data.tokens.TokenNotFoundError
    options:
      heading_level: 3

::: almanak.framework.data.tokens.AmbiguousTokenError
    options:
      heading_level: 3

## Symbol deprecation

Raised (3.0.0+) or warned (2.x) when a bare symbol is used where stable asset
identity is required. `SYMBOL_TOKEN_REMOVAL_VERSION` is the release that flips
the warning into an error.

::: almanak.framework.data.tokens.SymbolTokenResolutionError
    options:
      heading_level: 3

::: almanak.framework.data.tokens.SymbolTokenResolutionWarning
    options:
      heading_level: 3
