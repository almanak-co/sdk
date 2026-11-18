# Compound V3 Ethereum lending

> Compound V3 USDC Comet on Ethereum with two funds: a lender supplies USDC; a borrower posts WETH, borrows USDC, then repays in steps and withdraws.

This reference uses one explicitly configured USDC Comet on `eip155:1`. Named
`lender` and `borrower` funds have separate positions. `config.yaml` owns market,
asset, oracle, capital, borrowing grant and interval choices. The strategy
is `strategy.py`; `backtest-economic.yaml` separately selects historical inputs
and simulation policy.

The lender supplies 1000 USDC. The borrower supplies 1 WETH, borrows 200 USDC and
holds its proceeds. Teardown repays 100 USDC, releases 0.1 WETH, withdraws 100 USDC
from the lender, repays the remaining debt, then withdraws remaining collateral
and lender shares. A 20 USDC initial borrower buffer covers bounded interest.
Restart retains owned positions and pending operations. These are explicit fixture
quantities, not deployment defaults or investment recommendations.

Generated Safe/Zodiac authority belongs to each fund; operators submit and pay
gas. Comet ABI selectors cannot distinguish supply from repayment or withdrawal
from borrowing, so the gateway enforces those economic meanings.
Unknown external position changes stop native reconciliation. Existing generated
full-withdraw authority can release positive base credit after absorption, but
that permission proof does not imply automatic native absorption adoption.

The prepared model uses explicit Comet oracle observations and sampled integer
accrual, with immediate simulated liquidation and reported bad debt. The example
excludes execution costs explicitly. Missing borrower risk inputs leave its
obligations visible while independent lender cleanup can continue. Synthetic
stress tests are separate from historical evidence.

`almanak support --connector compound_v3 --chain eip155:1` states the
declaration; fork proofs use simulated funds.

The loan allocation accepts an optional
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) for the
interest the teardown's full repay needs.
