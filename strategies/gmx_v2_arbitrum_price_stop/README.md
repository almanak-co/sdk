# GMX V2 position with a price stop and take-profit on Arbitrum

> Open one GMX V2 WETH position and close it when the WETH price moves `stop_loss_bps` against it or `take_profit_bps` in its favour, read from on-chain one-minute pool closes.

The strategy opens one position from the allocation's `entry_collateral` and
`entry_size_usd_e30` (long or short by `side`), then reads the latest one-minute close of
the Arbitrum WETH/USDC Uniswap V3 pool every decision. The entry price is the position's
own, its USD size over its WETH size, so a restart needs no stored price.

| Move from entry (in the position's favour) | Decision |
|---|---|
| at or below `-stop_loss_bps` | close |
| at or above `take_profit_bps` | close |
| in between, or a stale series | hold |

A closed position is not reopened. Teardown, a failed request or a liquidation also
closes it; teardown needs no price.

## Parameters (`strategy.parameters`)

| Parameter | Meaning |
|---|---|
| `stop_loss_bps` | Adverse move from entry, in basis points, that closes the position. |
| `take_profit_bps` | Favourable move from entry, in basis points, that closes it. |

Both are integers from 1 to 10000. The pool close excludes fees and is not GMX's own
oracle price, so a stop triggers on the pool's price, not the GMX mark.
The allocation's acceptable prices are as wide as the position reference's; set them
for production. They suit a long: `increase_acceptable_price` is a ceiling above
`decrease_acceptable_price`. For `side: short` swap them, so the increase floor sits
below the decrease ceiling; `validate_config` refuses prices that don't match the side.

## Fork tests

A fork's pool price doesn't move between decisions, so no `stop_loss_bps` or
`take_profit_bps` fires there. A fork run covers the open, the holds and the teardown
close; `tests/test_authoring_templates.py` covers the stop and the take-profit offline.
