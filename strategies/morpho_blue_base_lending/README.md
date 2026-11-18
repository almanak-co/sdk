# Morpho Blue Base lending

> Morpho Blue on Base with two funds: a lender supplies USDC; a borrower posts WETH, borrows USDC, partially repays and withdraws, then fully exits.

This reference supplies USDC in a lender fund and supplies WETH collateral in a
separate borrower fund, borrows and holds USDC, then partially repays and withdraws
before full repayment and exits. The single author configuration selects the exact
market, financial assets, oracle and named funds.

`almanak support --connector morpho_blue --chain eip155:8453` states the
declaration; fork proofs use simulated funds.
Native EOA and Safe/Zodiac lifecycle evidence
and prepared economic evidence
include recovery and closure on identified forks and retained historical inputs.

`backtest-economic.yaml` selects the historical window, hypothetical inventory,
archive oracle risk, accounting sampling, source ages, collection limits and
excluded gas costs. Settings and inputs remain fixed across resume. The market
model preserves shared liquidity and bad debt across funds; its disclosed
archive-anchored assumptions do not establish realized historical fills.

The loan allocation accepts an optional
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) for the
interest the teardown's full repay needs.
