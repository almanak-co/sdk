# GMX V2 Avalanche position

> Fork-only GMX V2 long on Avalanche WAVAX/USDC with WAVAX collateral: open a sized position, wait for keeper settlement, hold, then request a full close.

This candidate reference opens one explicitly sized WAVAX-collateral long
position, waits for keeper settlement, holds and requests a full close. Author
inputs are `strategy.py` and `config.yaml`; generated deployment and credential
files are runtime artifacts. The market, collateral, entry size, fee and price
arguments are explicit configuration.

Actual identified-fork proofs exercise generated Safe/Roles and EOA accounts
through the shared runtime and dedicated gateway. Both retain positions through
runtime restart and gateway replacement. A separate Safe proof recovers an
accepted request after a coordinator broadcast crash and cancels its exact key
after the on-chain delay. Authentic captured signed reports pass the configured
deployed provider and verifier without oracle mutation.

The dedicated Safe uses the explicitly accepted broader cancellation grants.
An operator can cancel other compatible Safe-owned requests outside this
strategy. Gateway retained-key checks are a separate restriction.

GMX remains unpromoted. These proofs do not establish prepared economics,
remaining position/GM liquidity lifecycles or production execution.
