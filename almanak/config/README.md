# `almanak.config` — typed configuration service

The authoritative design lives in
[`docs/internal/config-service-plan.md`](../../docs/internal/config-service-plan.md).
This README is a quick-start pointer for contributors who land here from a
stack trace or an import.

## Quick start

```python
from almanak.config import load_config

config = load_config()  # called once at the relevant framework boot surface
```

`load_config()` returns either a `LocalConfig` or a `HostedConfig` depending
on `is_hosted()` (the `AGENT_ID` single-reader rule). Both share
`BaseConfig.gateway` so any code that only needs gateway settings can take
`BaseConfig` and ignore the discriminator. The currently-wired boot-time
submodels include `gateway`, `connectors`, `backtest`, `simulation`, `cli`,
`framework`, `safe_signer`, and `agent_tools`.

## Boot rule

- The framework owns `load_config()`. User strategy code should not import or
  call it.
- Each process entrypoint calls `load_config()` once after resolving the
  relevant dotenv source for that surface. Examples: CLI bootstrap, the
  `strat run` wrapper after loading a strategy-local `.env`, and
  `gateway.server.main()`.
- After boot, framework code should prefer injected typed slices from that
  object (for example `config.cli` or `config.framework`) instead of reparsing
  env ad hoc.
- Direct `*_config_from_env()` calls are transition shims. They are reserved
  for config-service assembly, truly dynamic env reads, or explicit
  compatibility paths that cannot use the boot-loaded object.

## Current status

The package is past the original Phase 0 skeleton. `load_config()` now builds
the currently-migrated typed submodels eagerly so normal boot-time consumers can
reuse one config object instead of reparsing env in each layer.

Some surfaces still expose compatibility adapters such as `*.from_env()` or
small helper functions. Those should be treated as transition shims:
- Boot-time consumers should prefer typed slices from `load_config()`.
- Truly dynamic values that must reflect live process state can remain helper
  APIs under `almanak.config`.

## Files

| File | Role |
|---|---|
| `__init__.py` | Public exports (`load_config`, `BaseConfig`, …) |
| `base.py` | `BaseConfig` + `GatewayConfig` alias for the gateway submodel |
| `local.py` | `LocalConfig` (folder-scoped, sqlite-backed) |
| `hosted.py` | `HostedConfig` (gateway-managed, postgres-backed) |
| `framework.py` | Framework toggles and boot-time runtime safety knobs |
| `safe_signer.py` | Safe wallet registry + Zodiac signer config |
| `simulation.py` | Tenderly / Alchemy simulation config |
| `demo_runtime.py` | Standalone demo `run_anvil.py` boundary helpers |
| `gateway_runtime.py` | Dynamic gateway-runtime helpers that intentionally still read live env |
| `backtest.py` | Backtesting + standalone backtest-service config |
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
