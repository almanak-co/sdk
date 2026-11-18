# Ethereum Curve StableSwap NG LP

> Curve StableSwap NG LP on Ethereum USDe/USDC: two funds swap, add, increase, partially remove, hold, then exit proportionally or to USDC.

Reference using the configured USDe/USDC pool and verified 2023-11-15 StableSwap
NG implementation. `almanak support --connector curve --chain eip155:1` states
the declaration; fork proofs use simulated funds.

The author config owns coin order, capital, entry slices and fund accounts;
providers and fork funding are operator inputs. Pool calls return proceeds to the fund. The
operator pays gas. Generated Roles permissions pin the pool, its coins and the
fund as receiver, with no amount limit; the gateway bounds each action by the
retained quote, slippage and attributed inventory. The gateway checks deadlines
because these pool entry points do not encode one. The strategy enters once:
it follows the entry quote only until `entry_complete`.

LP fees remain embedded in pool share economics. No separate fee collection or
fiat income is reported. The LP reference uses distinct funds for proportional
and explicitly selected single-coin exits. Initial funds cannot adopt unrelated
shares in this pool.
