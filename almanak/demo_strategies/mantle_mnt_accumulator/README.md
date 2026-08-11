# Mantle Mnt Accumulator (Demo)

Multi-signal MNT accumulation with Agni Finance on Mantle

## Chain

mantle

## Quick Start

```bash
almanak strat demo --name mantle_mnt_accumulator
cd mantle_mnt_accumulator
almanak strat run --network anvil --once
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.

`swap_fee_tier` (default `null`) optionally pins every accumulation swap to one
exact Agni fee tier (e.g. `500` for the 0.05% WMNT/USDT pool) instead of
letting the compiler pick the best-quoting tier each time; compilation fails
loudly if the pinned tier's pool is unusable. The teardown sweep stays
unpinned so closing the position never blocks on one pool's health.
