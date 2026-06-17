# Balancer

Connector for Balancer V2 — **flash loans only**.

!!! warning "Flash-loan-only — no LP support"
    Balancer is integrated for **`FLASH_LOAN`** intents (via the Balancer Vault),
    **not** liquidity provision. `Intent.lp_open(..., protocol="balancer")` /
    `lp_close(...)` are **not** routable and fail at compile time with a
    capability error listing the protocols that do support LP
    (e.g. Uniswap V3/V4, Aerodrome, Curve, TraderJoe V2, …).

    For Balancer-pool LP, no connector exists yet. The authoritative,
    intent-scoped capability list is `almanak info matrix` (Balancer appears
    under the `flash_loan` category only).

::: almanak.connectors.balancer_v2
    options:
      show_root_heading: true
      members_order: source
