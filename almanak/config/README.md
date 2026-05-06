# `almanak.config` — typed configuration service

The authoritative design lives in
[`docs/internal/config-service-plan.md`](../../docs/internal/config-service-plan.md).
This README is a quick-start pointer for contributors who land here from a
stack trace or an import.

## Quick start

```python
from almanak.config import load_config

config = load_config()  # called once at the Click main group
```

`load_config()` returns either a `LocalConfig` or a `HostedConfig` depending
on `is_hosted()` (the `AGENT_ID` single-reader rule). Both share
`BaseConfig.gateway` so any code that only needs gateway settings can take
`BaseConfig` and ignore the discriminator.

## Phase 0 status

This package currently exposes only the gateway submodel — `GatewayConfig` is
an alias for `GatewaySettings` (`almanak/gateway/core/settings.py`). Mode-
specific fields (private key, db path, agent_id, simulation, etc.) land in
later phases as the plan migrates each surface.

Phase 0 ships the skeleton, the parity test that guards the cutover, and the
CI lint gate that enforces the boundary going forward. Behavior change: none.

## Files

| File | Role |
|---|---|
| `__init__.py` | Public exports (`load_config`, `BaseConfig`, …) |
| `base.py` | `BaseConfig` + `GatewayConfig` alias for the gateway submodel |
| `local.py` | `LocalConfig` (folder-scoped, sqlite-backed) |
| `hosted.py` | `HostedConfig` (gateway-managed, postgres-backed) |
| `strategy.py` | `StrategyConfig` Pydantic base for per-strategy schemas |
| `service.py` | `load_config()` + the singleton `_load_dotenv_once()` |
| `env.py` | The single env-collection layer (re-exports `_load_dotenv_once`) |
| `cli_options.py` | Click decorators (filled in Phase 2) |
| `precedence.py` | Documented and unit-tested precedence rule |

## Tracking issues

* [#2097](https://github.com/almanak-co/almanak-sdk-private/issues/2097) — gateway / framework env divergence (Phase 1)
* [#2098](https://github.com/almanak-co/almanak-sdk-private/issues/2098) — strategy `config.json` schema (Phase 3)
* [#2099](https://github.com/almanak-co/almanak-sdk-private/issues/2099) — CLI option precedence (Phase 2)
* [#2100](https://github.com/almanak-co/almanak-sdk-private/issues/2100) — `os.environ` mid-run mutation (Phase 4)
* [#2101](https://github.com/almanak-co/almanak-sdk-private/issues/2101) — `config.json` parsed multiple times (Phase 3)
