# Arbitrum Morpho Blue borrowing and Uniswap V3 trading

> Composed Morpho Blue borrow then one Uniswap V3 swap on Arbitrum: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

This reference reuses the lending and trading strategy with explicitly configured
Arbitrum contracts and oracle inputs. It supplies 0.01 WETH, borrows 5 USDC and
trades 2.5 of them; 2 initial USDC are a repayment reserve. Borrowed USDC funds a
WETH round trip.
Teardown closes the trade, repays debt and withdraws collateral.

The configuration defines the selected Morpho market and pool; neither is a
connector default. `almanak support --connector morpho_blue --chain eip155:42161` and `almanak support --connector uniswap_v3 --chain eip155:42161` state the declarations; fork proofs use simulated funds.

## The loan-asset reserve

The 2 USDC in `inventory` beside the trade's 2.5 USDC `capital` slice is the user's own
loan-asset reserve: the swap sells only the slice, so the reserve stays unsupplied in the
fund. Interest is not prefunded and the round trip pays the pool fee twice, so the
borrowed USDC alone cannot settle the full repay; the reserve covers that, and without
it the repay refuses with `repayment_funding` until loan asset is sent to the fund. Size
it to the expected interest plus fees; the fork operator file funds it. The same
inventory pattern adds a reserve to any borrow-and-hold strategy built from this one.
