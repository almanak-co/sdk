# Avalanche Morpho Blue borrowing and Uniswap V4 trading

> Composed Morpho Blue borrow then one hookless Uniswap V4 swap on Avalanche: supply stAVAX, borrow WAVAX, swap half to USDC, hold, swap back, repay, withdraw.

One named fund supplies 1 stAVAX as Morpho Blue collateral, borrows 0.1 WAVAX
from the same explicitly configured market and trades 0.05 WAVAX through the
configured Uniswap V4 WAVAX/`0xb97e...48a6e` fee-500 hookless pool. The fund
starts with an explicit zero balance of the pool counter-asset, so reversal
cannot consume pre-existing cash, and 0.011 WAVAX as the repayment interest
buffer in addition to the untraded proceeds. The buffer covers the frozen-pool
round-trip fees, which are charged on every reversal, so the retained wallet
always covers the debt and its interest. Teardown reverses the trade, repays
the debt and withdraws collateral in that order. Failed reversal retains the
lending obligation. The runtime owns shared cash and protocol positions
separately from strategy memory.

The Morpho market (core, market ID, WAVAX loan asset, stAVAX collateral,
ERC4626 conversion oracle, adaptive IRM, LLTV) is the one configured for
`morpho_blue_avalanche_lending`; both operator fork fixtures pin the same block. The pool and periphery
contracts are the ones configured for `avalanche_aave_v4_borrow_and_trade`.
Display symbols are presentation metadata; asset identity is the configured
CAIP-19 reference. stAVAX fixture capital uses the compiled proxy layout in
`almanak.gateway.morpho_avalanche_fixture`; only identified local forks may
fund the reference or install generated Safe/Zodiac authority.

Public author validation accepts the configuration.
`almanak support --connector morpho_blue --chain eip155:43114` and `almanak support --connector uniswap_v4 --chain eip155:43114` state the declarations; fork proofs use simulated funds.
