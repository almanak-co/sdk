# Market Snapshot

::: almanak.framework.market

## Overview

`almanak.framework.market` is the canonical home for `MarketSnapshot` — the
strategy-facing market-data interface. It replaces the two legacy locations
(`almanak.framework.strategies.intent_strategy.MarketSnapshot` and
`almanak.framework.data.market_snapshot.MarketSnapshot`) that silently
diverged before VIB-4062.

## Builder factories

::: almanak.framework.market.builders.MarketSnapshotBuilder

## Typed errors

::: almanak.framework.market.errors

## Return-type DTOs

::: almanak.framework.market.models

## Provider Protocols (sync adapters)

::: almanak.framework.market.services
