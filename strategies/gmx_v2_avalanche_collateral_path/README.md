# GMX V2 Avalanche collateral path

> Fork-only GMX V2 collateral management on Avalanche WAVAX/USDC: open, increase, add then withdraw collateral at fixed notional, partially decrease, hold and close.

Candidate reusable strategy: open, increase, add collateral without changing
notional, withdraw the added collateral without changing notional,
partially decrease, hold and close. Each entry after the first waits for a new
chain timestamp after the previous completed request. Actual long/short proofs passed through Safe/Roles and EOA, including both
restarts, funding cleanup and direct Safe permission rejection checks.
This remains a candidate and does not establish SDK support.
