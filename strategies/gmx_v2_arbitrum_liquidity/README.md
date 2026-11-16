# GMX V2 Arbitrum GM liquidity

> GMX V2 GM-pool liquidity on Arbitrum: deposit for GM shares, increase, partially withdraw, hold, withdraw the rest and revoke any residual approval.

This reference uses explicitly configured market currencies, GM-share output and
withdrawal bounds. It opens, increases, partially withdraws, holds through
restart, closes remaining GM shares and revokes any residual token authority.
Approvals are sized to each request and normally consumed in full, so no
revocation is usually needed. Both withdrawn currencies remain owned inventory.
Fees embedded in GM value are not separately claimable inventory.
`almanak support --connector gmx_v2 --chain eip155:42161` states the
declaration; fork proofs use simulated funds.
