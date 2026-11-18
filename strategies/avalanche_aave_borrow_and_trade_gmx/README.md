# Avalanche Aave borrowing and GMX trading candidate

> Fork-only composed Aave V3 borrow then one GMX V2 swap on Avalanche: supply USDC, borrow WAVAX, keeper-settled swap of half, hold, swap back, repay, withdraw.

One explicitly named fund supplies 100 USDC as Aave V3 collateral, borrows
1 WAVAX and sends 0.5 WAVAX through the configured GMX market. It retains the
measured output, returns that quantity, revokes any residual authority, repays and
withdraws collateral. The initial 2.1 WAVAX includes the two explicit 1 WAVAX
keeper execution fees and repayment headroom. Native refunds remain shared cash.

The strategy body matches the Arbitrum reference apart from the borrow amount. Its
single author configuration selects Avalanche assets, oracle feeds, markets and
funding; providers and the fork fixture are operator inputs. The separate prepared configuration selects a 50-second decision window
and 250-second teardown window with explicit signed-report history. The captured
opportunities occur at offsets 5, 30 and 73 seconds, so teardown must begin before
the final opportunity. Missing
settlement opportunities leave retained escrow and an incomplete result.

This reference is a candidate. Actual generated Safe fork, prepared reproduction
and forced-process recovery proofs are required before promotion. No production
permission installation or real spending is authorized.
