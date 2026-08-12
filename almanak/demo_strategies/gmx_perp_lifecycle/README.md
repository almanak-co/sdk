# GMX V2 lifecycle diagnostic

This advanced demo exercises GMX V2 asynchronous order submission,
cancellation, replacement, keeper settlement, and restart recovery. It is a
connector-conformance scenario rather than the recommended structure for a new
trading strategy.

For strategy authoring, start from the `perps` scaffold or
`gmx_v2_directional_perp`. Those references make the ordinary decision,
persistence, and teardown flow easier to audit.

The lifecycle demo still follows the canonical teardown contract:

- all price and collateral valuation reads use chain-specific token addresses;
- `probe_perp_position()` converts raw `PerpsPositionOnChain` rows into
  normalized `PerpProbePosition` values;
- OPEN, FLAT, and UNMEASURED remain distinct;
- venue-reported side and collateral identify each close;
- `size_usd=None` closes the full live position;
- a submitted close is observed rather than duplicated.

See `almanak/demo_strategies/REFERENCE.md` and the public Strategies API
documentation before adapting a packaged demo.
