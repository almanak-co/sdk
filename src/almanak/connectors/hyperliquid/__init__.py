"""Hyperliquid's HyperCore, a venue of `eip155:999`, by the hybrid plan of blueprint 30.

Implemented: HyperCore observation (perpetual market resolution and the identity of
spot balances), custody (the CoreWriter Roles grants, D9 agent approval and renewal, and
custody requests attributed by their own effects), agent-signed trading attempts with
their admission and recovery, the reconciled perp book, and a synthesized HyperCore
model for forks.

The `hyperliquid` connector runs backtests (section 12) and fork runs (section 13), and
both modes share one strategy surface: phase 5's configuration, market resolution,
intents and capabilities. Fork runs add the prepared configuration, generated custody
permissions, the checkpoint that holds the custody, trading and book documents,
planning of each intent's custody and venue steps, custody requests and external
effects, the `HyperCoreGateway` adapter that signs and posts venue actions, and the
`hypercore-model` venue process of the fork workspace. Production waits for D7.
"""
