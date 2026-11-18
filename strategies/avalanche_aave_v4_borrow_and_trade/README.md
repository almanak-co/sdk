# Avalanche Aave V4 borrowing and Uniswap V4 trading

> Composed Aave V4 borrow then one hookless Uniswap V4 swap on Avalanche: supply WETH.e, borrow USDC, swap part to WAVAX, hold, swap back, repay, withdraw.

One named fund supplies 0.01 WETH.e collateral, borrows 5 USDC and trades
2.5 USDC for WAVAX through the configured Uniswap V4 pool. The fund starts
with zero WAVAX, so reversal cannot consume pre-existing WAVAX cash. One initial
USDC remains an explicit repayment buffer in addition to the untraded proceeds.
The trade reverses before repayment and collateral withdrawal. Failed reversal
retains the lending obligation. The runtime owns shared cash and protocol
positions separately from strategy memory.

The Main Spoke WAVAX reserve was above its supply cap at the measured current
and archive boundaries. The explicit WETH.e reserve has available supply
capacity; selecting it does not alter the configured trading pool.

`almanak support --connector aave_v4 --chain eip155:43114` and `almanak support --connector uniswap_v4 --chain eip155:43114` state the declarations; fork proofs use simulated funds.

## Aave V4: full repay and full withdraw only

The SDK's Aave V4 connector offers only a full withdraw and a full repay, so the close is the trade's exit
swap (only while `traded`), one full repay, then one full withdraw; nothing runs after
the withdraw, so withdrawn collateral is never swapped. A shortfall cannot be covered by
releasing part of the collateral first: the unsupplied loan-asset buffer is how a V4
strategy pays accrued interest and fees. The 1 USDC in `inventory` is that buffer; the
swap takes only the trade's `capital` slice, so it stays in the fund. The same shape
serves a V4 short: supply part of the collateral asset and keep the rest unsupplied to
buy the borrowed asset back, then repay in full and withdraw.
