# Token Resolution

Unified token resolution for addresses, decimals, and symbol lookups across all chains.

## Usage

```python
from almanak.framework.data.tokens import get_token_resolver

resolver = get_token_resolver()

# Resolve by symbol
token = resolver.resolve("USDC", "arbitrum")
print(token.address, token.decimals)  # 0xaf88... 6

# Resolve by address
token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

# Convenience methods
decimals = resolver.get_decimals("arbitrum", "USDC")
address = resolver.get_address("arbitrum", "USDC")

# For DEX swaps (auto-wraps native tokens: ETH->WETH, etc.)
token = resolver.resolve_for_swap("ETH", "arbitrum")
```

## get_token_resolver

::: almanak.framework.data.tokens.get_token_resolver
    options:
      show_root_heading: true

## TokenResolver

::: almanak.framework.data.tokens.TokenResolver
    options:
      show_root_heading: true
      members_order: source

## ResolvedToken

::: almanak.framework.data.tokens.ResolvedToken
    options:
      show_root_heading: true

## BridgeType

::: almanak.framework.data.tokens.BridgeType
    options:
      show_root_heading: true

## Exceptions

::: almanak.framework.data.tokens.TokenResolutionError
    options:
      show_root_heading: true

::: almanak.framework.data.tokens.TokenNotFoundError
    options:
      show_root_heading: true

::: almanak.framework.data.tokens.AmbiguousTokenError
    options:
      show_root_heading: true
