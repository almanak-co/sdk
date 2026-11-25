# Bridge USDC to Base and lend what arrives

> Bridge a stated amount of USDC from an Arbitrum account to a Base account once through Across, then supply exactly the amount that arrived to Compound V3 on Base.

This reference composes two connectors over two named accounts on two chains.
`config.yaml` names a bridge market (USDC on `eip155:42161` to USDC on
`eip155:8453`, with the fee ceiling `max_fee_bps` and the quote source
`across_api`) and a Compound V3 market on Base. The bridge allocation `move`
names the account that sends and the `destination_account` that receives. The
lending allocation `earn` belongs to the receiving account.

`inventory` states the owned cash of both accounts: the amount to bridge on the
Arbitrum account and `0` USDC on the Base account. A composed bridge must
deliver into an inventory entry, and an allocation on the receiving account must
hold that asset.

The strategy is `strategy.py`. It is evaluated once per chain:

- On the Arbitrum turn it requests one `bridge` of `bridge_quantity`. The
  amount leaves the Arbitrum account's owned inventory when the deposit is
  mined.
- While the transfer is in flight the amount is in neither account's inventory.
  The gateway refuses a supply sized from the bridge's offer, because the Base
  account does not own it yet.
- `on_outcome` receives the measured arrival on the Base turn. From then on the
  received amount is owned inventory of the Base account, and the strategy
  supplies exactly that amount to the lending market.
- If nobody fills the transfer, the refund is measured on the Arbitrum turn and
  the Arbitrum account's inventory is whole again. Nothing is supplied.

Teardown withdraws the supply on Base and leaves the funds there. It waits for
an open transfer to arrive or be refunded, never bridges back, and requests
`revoke` only for an approval whose deposit was never mined.

The amount received is Across's own quote. The gateway refuses a quote whose fee
is above `max_fee_bps` before anything is signed. The ceiling is a gateway
check, not on-chain enforcement.

`backtest-economic.yaml` runs the same hooks in an economic backtest. The bridge
is modeled: it pays the whole `max_fee_bps`, arrives after the
`arrival_latency_seconds` the file states and is never refunded. The supply
reads Comet history. Every bridge amount in the report is labeled modeled.

Limits: the reference runs with EOA accounts; a Safe/Roles bridge is declared
and has never run. Across declares fork runs, production and
economic backtests on Arbitrum, and Compound V3 declares production on
Base, so this reference is part of the production rehearsal. Its operator example
is the fork file only. In production the gateway signs Across's own quote from its
API. On a fork no venue is asked: the fork gateway constructs
the quote from a stated fee and the fork's own block time, and it is retained
and reported under the source `across_fork_constructed`. A refund on a fork is a
fork-constructed bundle and proves only how the SDK reads one.

`almanak support --connector across --chain eip155:42161` states the bridge's
declaration; fork proofs use simulated funds.
