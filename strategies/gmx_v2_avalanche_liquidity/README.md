# GMX V2 Avalanche GM liquidity

> Fork-only GMX V2 GM-pool liquidity on Avalanche WAVAX/USDC: deposit for GM shares, increase, partially withdraw, hold, withdraw the rest and revoke any residual approval.

This candidate uses explicitly configured market currencies, GM-share output and
withdrawal bounds; it runs only on an identified fork. It opens, increases, partially
withdraws, holds through restart, closes remaining GM shares and revokes any
residual token authority. Approvals are sized to each request and normally
consumed in full, so no revocation is usually needed. Both withdrawn currencies
remain owned inventory. Fees embedded in GM value are not separately claimable
inventory. Real-fund spending is not authorized.
