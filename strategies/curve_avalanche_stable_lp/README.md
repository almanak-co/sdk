# Avalanche Curve StableSwap NG LP

> Curve StableSwap NG LP on Avalanche USDC/USDS: two funds swap, add, increase, partially remove, hold, then exit proportionally or to USDC.

This reference swaps USDC into USDS, opens a fungible LP position, increases it,
withdraws part, holds, and exits. Two independently named funds demonstrate
proportional and explicitly selected single-coin removal. Each starts with
0.1 USDC. Allocation names do not partition the pool's underlying token balances.

The configured market is `eip155:43114` pool
`0xa9d7d3d7e68a0cae89fb33c736199172f405c8d3`, registered in factory
`0x1764ee18e8b3cca4787249ceb249356192594585`. Its ordered coins are native
USDC `eip155:43114/erc20:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e`
and USDS `eip155:43114/erc20:0x86ff09db814ac346a7c6fe2cd648f27706d1d470`.
The gas payer's native asset is `eip155:43114/slip44:9005`. Symbols are display
labels, and neither token is assigned an assumed dollar peg.

`config.yaml` owns strategy parameters, explicit pool selection and named fund
accounts; providers and the fork fixture are operator inputs. The ordinary author input
remains this file plus `strategy.py`. `backtest-economic.yaml` separately selects
historical sampling, source age, limits, starting inventory, latency and the
teardown window. Generated manifests and databases are runtime artifacts.

The selected plain pool matches the December 12, 2023 StableSwap NG revision.
Both coins have standard transfer semantics at the retained implementations.
USDC has pause, blacklist and upgrade authority; USDS has authorized mint/burn
and UUPS upgrade authority. These external controls are not historical execution
access guarantees. Fork funding verifies native USDC's implementation and
balance layout before applying simulated capital. Generated Safe/Roles permissions
come from the sandboxed capability hook; only identified fork fixtures install them.

Prepared execution consumes archive-derived pool state without network calls.
Integer fills include fees and size impact. Subsequent external history stays
fixed despite hypothetical trades and liquidity. Costs are explicitly excluded;
fee accrual remains embedded in per-share pool economics. Missing observations,
unsupported revisions and source-age gaps fail closed. This reference selects
no vendor price feed and makes no dollar-denominated return claim.

`almanak support --connector curve --chain eip155:43114` states the declaration;
fork proofs use simulated funds. An empty TwoCrypto or TriCrypto factory pool is not support for its
family, and this stable reference proves neither crypto family.
