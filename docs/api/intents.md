# Intents

The intent vocabulary - high-level descriptions of what a strategy wants to do. The framework compiles these into executable transactions.

## Intent

Factory class for creating intents.

::: almanak.framework.intents.Intent
    options:
      show_root_heading: true

## IntentType

::: almanak.framework.intents.IntentType
    options:
      show_root_heading: true

## HoldIntent

::: almanak.framework.intents.HoldIntent
    options:
      show_root_heading: true

## SwapIntent

::: almanak.framework.intents.SwapIntent
    options:
      show_root_heading: true

## LPOpenIntent

::: almanak.framework.intents.LPOpenIntent
    options:
      show_root_heading: true

## LPCloseIntent

::: almanak.framework.intents.LPCloseIntent
    options:
      show_root_heading: true

## CollectFeesIntent

::: almanak.framework.intents.CollectFeesIntent
    options:
      show_root_heading: true

## BorrowIntent

::: almanak.framework.intents.BorrowIntent
    options:
      show_root_heading: true

## RepayIntent

::: almanak.framework.intents.RepayIntent
    options:
      show_root_heading: true

## SupplyIntent

::: almanak.framework.intents.SupplyIntent
    options:
      show_root_heading: true

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

## WithdrawIntent

::: almanak.framework.intents.WithdrawIntent
    options:
      show_root_heading: true

## DeleverageIntent

::: almanak.framework.intents.DeleverageIntent
    options:
      show_root_heading: true

## FlashLoanIntent

::: almanak.framework.intents.FlashLoanIntent
    options:
      show_root_heading: true

## PerpOpenIntent

::: almanak.framework.intents.PerpOpenIntent
    options:
      show_root_heading: true

## PerpCloseIntent

::: almanak.framework.intents.PerpCloseIntent
    options:
      show_root_heading: true

## StakeIntent

::: almanak.framework.intents.StakeIntent
    options:
      show_root_heading: true

## UnstakeIntent

::: almanak.framework.intents.UnstakeIntent
    options:
      show_root_heading: true

## BridgeIntent

::: almanak.framework.intents.BridgeIntent
    options:
      show_root_heading: true

## WrapNativeIntent

::: almanak.framework.intents.WrapNativeIntent
    options:
      show_root_heading: true

## UnwrapNativeIntent

::: almanak.framework.intents.UnwrapNativeIntent
    options:
      show_root_heading: true

## VaultDepositIntent

::: almanak.framework.intents.VaultDepositIntent
    options:
      show_root_heading: true

## VaultRedeemIntent

::: almanak.framework.intents.VaultRedeemIntent
    options:
      show_root_heading: true

## PredictionBuyIntent

::: almanak.framework.intents.PredictionBuyIntent
    options:
      show_root_heading: true

## PredictionSellIntent

::: almanak.framework.intents.PredictionSellIntent
    options:
      show_root_heading: true

## PredictionRedeemIntent

::: almanak.framework.intents.PredictionRedeemIntent
    options:
      show_root_heading: true

## EnsureBalanceIntent

::: almanak.framework.intents.EnsureBalanceIntent
    options:
      show_root_heading: true

## IntentSequence

::: almanak.framework.intents.IntentSequence
    options:
      show_root_heading: true

## ChainedAmount

::: almanak.framework.intents.ChainedAmount
    options:
      show_root_heading: true
