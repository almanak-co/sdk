# Morpho Blue Avalanche lending

> Morpho Blue on Avalanche with two funds: a lender supplies WAVAX; a borrower posts stAVAX, borrows WAVAX, partially repays and withdraws, then fully exits.

This reference supplies 1 WAVAX from the lender and 1 stAVAX as borrower
collateral, borrows 0.1 WAVAX, holds the proceeds, then partially repays and
withdraws before repaying all debt, using 0.01 WAVAX the fork fixture funds for
interest, and withdrawing both positions. Fund accounts are independent
from the operator paying gas. The configured market and conversion oracle are
explicit; there is no protocol-selected token, pool or financial source.

Providers, account bindings and the fork fixture are operator inputs in
[examples/operators](../../examples/operators/). Fork runs fund the reference and
install generated Safe/Zodiac authority.

The prepared economic configuration records the selected Morpho oracle at each
eligible historical block. It retains the conversion price in WAVAX base units
per stAVAX base unit, ordinary supply/borrow shares and immediate Morpho
liquidation semantics. Missing data fails acquisition. Simulated balances do
not change later external market history.

`almanak support --connector morpho_blue --chain eip155:43114` states the
declaration; fork proofs use simulated funds.
