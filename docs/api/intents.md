# Intents

The intent vocabulary - high-level descriptions of what a strategy wants to do. The framework compiles these into executable transactions.

## Intent

Factory class for creating intents.

::: almanak.framework.intents.Intent
    options:
      heading_level: 2
      filters:
        - "!^_" # keep the generic serialize/deserialize dispatchers; only the per-model copies below are suppressed

::: almanak.framework.intents.IntentType
    options:
      heading_level: 2

::: almanak.framework.intents.HoldIntent
    options:
      heading_level: 2

::: almanak.framework.intents.SwapIntent
    options:
      heading_level: 2

::: almanak.framework.intents.LPOpenIntent
    options:
      heading_level: 2

::: almanak.framework.intents.LPCloseIntent
    options:
      heading_level: 2

::: almanak.framework.intents.CollectFeesIntent
    options:
      heading_level: 2

::: almanak.framework.intents.BorrowIntent
    options:
      heading_level: 2

::: almanak.framework.intents.RepayIntent
    options:
      heading_level: 2

::: almanak.framework.intents.SupplyIntent
    options:
      heading_level: 2

### Exact Pool / Comet binding

For Aave V3 and Compound V3 supply and withdraw operations, pass
`expected_pool="0x..."` to assert the exact Pool (Aave) or Comet (Compound)
address supplied by an upstream verifier. The SDK still routes through its
connector-owned canonical registry; compilation fails before approval or
protocol calldata construction if the registry-selected venue does not match.
The assertion is validated as a 20-byte EVM address and normalized to checksum
form. It is never used as a routing override.

Compound V3 also accepts the Comet address as `market_id`. That form resolves
to the catalogue key (`"weth"`, `"usdc"`, …); an unknown address fails closed.

```python
from decimal import Decimal

from almanak.framework.intents import Intent


intent = Intent.supply(
    protocol="aave_v3",
    token="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    amount=Decimal("100"),
    chain="polygon",
    expected_pool="0x794a61358D6845594F94dc1DB02A252b5b4814aD",
)
```

::: almanak.framework.intents.WithdrawIntent
    options:
      heading_level: 2

::: almanak.framework.intents.DeleverageIntent
    options:
      heading_level: 2

::: almanak.framework.intents.FlashLoanIntent
    options:
      heading_level: 2

::: almanak.framework.intents.PerpOpenIntent
    options:
      heading_level: 2

::: almanak.framework.intents.PerpCloseIntent
    options:
      heading_level: 2

::: almanak.framework.intents.StakeIntent
    options:
      heading_level: 2

::: almanak.framework.intents.UnstakeIntent
    options:
      heading_level: 2

::: almanak.framework.intents.BridgeIntent
    options:
      heading_level: 2

::: almanak.framework.intents.WrapNativeIntent
    options:
      heading_level: 2

::: almanak.framework.intents.UnwrapNativeIntent
    options:
      heading_level: 2

::: almanak.framework.intents.VaultDepositIntent
    options:
      heading_level: 2

::: almanak.framework.intents.VaultRedeemIntent
    options:
      heading_level: 2

::: almanak.framework.intents.PredictionBuyIntent
    options:
      heading_level: 2

::: almanak.framework.intents.PredictionSellIntent
    options:
      heading_level: 2

::: almanak.framework.intents.PredictionRedeemIntent
    options:
      heading_level: 2

::: almanak.framework.intents.EnsureBalanceIntent
    options:
      heading_level: 2

::: almanak.framework.intents.IntentSequence
    options:
      heading_level: 2

::: almanak.framework.intents.ChainedAmount
    options:
      heading_level: 2
