# GMX V2 Avalanche position path

> Fork-only GMX V2 position sizing on Avalanche WAVAX/USDC: open, increase, partially decrease, hold and close one long (or short) position, each step keeper-settled.

Candidate reference: open, increase by the same configured size and collateral,
partially decrease by that size, hold and close. Each request waits for actual
keeper completion. Capital covers the sequence's collateral and keeper fees.
The configured side is long; the same source accepts a short configuration.
Actual runtime proofs passed for long and short through Safe/Roles and EOA,
including both restarts and funding cleanup. No support promotion is claimed.
