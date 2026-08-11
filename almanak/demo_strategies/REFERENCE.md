# Strategy authoring references

Packaged demos serve two different purposes. Some are canonical examples for
authors; others are protocol conformance or lifecycle diagnostics. Choose a
canonical example before adapting strategy code.

## Canonical perpetual references

| Use case | Reference |
|---|---|
| New perpetual strategy | Start with `almanak strat new --template perps`. Its generated teardown is the canonical contract. |
| GMX V2 directional strategy | `gmx_v2_directional_perp` |
| Hyperliquid directional/trailing strategy | `hyperliquid_trailing_perp` |

These references use `probe_perp_position()` for venue-derived teardown,
consume `PerpProbePosition.notional_usd` only after normalization, preserve the
OPEN/FLAT/UNMEASURED distinction, and close with `size_usd=None`.

## Diagnostic perpetual demos

`gmx_perp_lifecycle` exercises GMX asynchronous submission, cancellation,
replacement, and keeper settlement. It implements the same normalized teardown
contract, but its multi-phase state machine is intentionally more complex than
a normal strategy and should not be used as the structural starting point for
generated strategies.

Never combine direct `MarketSnapshot.perp_positions()` rows with fields copied
from a probe-based example. The direct call returns raw
`PerpsPositionOnChain` objects; strategy teardown should consume the normalized
probe instead. See the public Strategies API documentation for the full type
contract and testing requirements.
