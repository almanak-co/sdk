# Across USDC bridge, Arbitrum to Base

> Bridge a stated amount of USDC from an Arbitrum account to a Base account once through Across, and record whether it arrived or was refunded.

This reference binds two named accounts of one strategy on two chains.
`config.yaml` names one bridge market, which is one direction: USDC on
`eip155:42161` to USDC on `eip155:8453`, with the author's fee ceiling
`max_fee_bps` and the quote source `across_api`. The allocation names the account
that sends and the `destination_account` that receives.

The strategy is `strategy.py`. On the source chain's turn it requests one
`bridge` of `bridge_quantity`. The connector approves the SpokePool for the exact
amount, sends the quoted deposit and clears any residue. On the destination
chain's turn the strategy observes no allocation and requests nothing. Until
the transfer ends the source account holds a transfer in flight, not a balance.

`on_outcome` records three things in memory: what left the source account, what
was measured on the destination account when a relayer filled the transfer, or
what the venue paid back to the source account when nobody did. A refund is read
only after the destination chain has shown that no fill can follow. Teardown
waits for the transfer to end and does not bridge back. It requests `revoke` only
when an approval is left whose deposit was never mined.

The amount received is Across's own quote. The gateway refuses a quote whose fee
is above `max_fee_bps` before anything is signed. The ceiling is a gateway
check, not on-chain enforcement.

Limits: the source account is an EOA. Fork runs are declared for an Arbitrum
source only, and there is no backtest model and no production support. On a
fork no venue is asked: the fork gateway constructs the quote from a stated fee
and the fork's own block time, and labels it. A refund on a fork is a
fork-constructed bundle and proves only how the SDK reads one.

`almanak support --connector across --chain eip155:42161` states the declaration;
fork proofs use simulated funds.
