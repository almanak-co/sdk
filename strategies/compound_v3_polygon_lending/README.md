# Compound V3 Polygon lending candidate

> Blocked Compound V3 candidate on the Polygon USDC.e Comet with two funds: a lender supplies USDC.e; a borrower posts WETH and borrows, refused by zero caps.

This candidate mirrors the Optimism reference: the lender fund supplies the
configured base asset and the borrower fund borrows it against WETH collateral,
exercising partial and full repayment and withdrawal with independent fund
ownership. The author configuration names the exact `eip155:137` market and
financial sources.

The configured market is the documented bridged-USDC Comet
`0xf25212e676d1f7f89cd72ffee66158f541246445` with base asset
`eip155:137/erc20:0x2791bca1f2de4661ed88a30c99a7a9449aa84174` and WETH
collateral `eip155:137/erc20:0x7ceb23fd6bc0add59e62ac25578270cff1b9f619`.

The base and collateral price feeds are the ones listed by the official
deployment configuration; the Compound reader
correlates the configured base feed with the compiled implementation constructor
and refuses a mismatch. The fork target reuses the identified Polygon block
pinned by `morpho_blue_polygon_lending`. The economic window starts at the
retained archive boundary of 2026-09-07 15:00 UTC.

This is a blocked candidate reference, not advertised SDK support. Both
documented Polygon markets (the bridged-USDC market above and the USDT market
`0xaeb318360f27748acb200ce616e389a6c9409a07`) had zero collateral supply caps
for every observed collateral asset at the retained current and archive
boundaries, so the promised supply, borrow, repay and withdraw lifecycle cannot
complete: the gateway refuses the borrower's collateral supply before borrowing.
Compound V3 does not declare `eip155:137`, so public author validation refuses
this configuration. The gateway also has no verified storage fixture for the
bridged USDC.e proxy; `local_storage` funding of the base asset needs that
fixture before any fork lifecycle can start. Reopen when a canonical supported
Comet has nonzero capacity for an eligible collateral asset, then run all
native, prepared, authority, recovery and composition proofs. No protocol caps
are overridden and no production authority is granted.
