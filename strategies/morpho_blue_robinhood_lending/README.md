# Morpho Blue Robinhood lending

> Morpho Blue on Robinhood Chain with two funds: a lender supplies USDG; a borrower posts WETH, borrows USDG, partially repays and withdraws, then fully exits.

Separate named funds supply USDG and borrow it against WETH collateral, then partially repay and release supply before full cleanup. The configuration specifies the exact `eip155:4663` Morpho market, non-vault oracle, adaptive IRM and CAIP assets. Protocol positions, inventory and gas payers retain explicit scope.

USDG ERC-20 balances exclude unclaimed rewards. This reference claims no rewards. Its local fixture funds only fresh, unfrozen payout-group-zero accounts against independently compiled and canonically verified packed balance storage; no token policy is overridden. Generated Safe/Zodiac authority is installed only on identified forks. EOA proofs use the same gateway path. The separate economic backtest prepares explicit archival inputs before offline execution. Retained reference proofs cover native EOA and generated Safe/Roles lifecycles, process recovery and isolated prepared reproduction. `almanak support --connector morpho_blue --chain eip155:4663` states the declaration; fork proofs use simulated funds.
