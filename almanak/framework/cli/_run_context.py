"""Shared state carriers for `almanak strat run`.

`RunContext` accumulates mutable runtime handles as the strategy-run bootstrap
progresses. The frozen `IdentityInfo`, `ResumeInfo`, `StrategyBootstrap`, and
`RuntimeBootstrap` dataclasses carry the immutable outputs of specific
bootstrap decisions so the orchestrator can thread them without long tuples.

Runtime-handle fields that carry protocol-specific types live in `Any = None`
slots. Typed fields get explicit types. Keep this module focused on state
carriers, not behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunContext:
    """Mutable state carrier threaded through `run()` bootstrap helpers.

    Populated progressively. Fields appear only as the helper that writes
    them lands. Runtime-handle fields typed as `Any = None` because their
    concrete types (`GatewayClient`, `ManagedGateway`, strategy class) pull
    in heavy imports or create circular-import risk at the helper boundary.
    """

    # Gateway (populated by `_setup_gateway`)
    gateway_client: Any = None
    managed_gateway: Any = None
    gateway_host: str = "localhost"
    gateway_port: int = 50051
    gateway_network: str = "mainnet"
    session_auth_token: str | None = None
    isolated_wallet_address: str | None = None

    # Strategy (populated between phases in `run()`)
    strategy_class: Any = None
    strategy_config: dict[str, Any] = field(default_factory=dict)
    strategy_chains: list[str] = field(default_factory=list)
    multi_chain: bool = False


@dataclass(frozen=True)
class IdentityInfo:
    """Output of `_resolve_identity`.

    deployment_id: stable primary key for all state tables.
    run_id: per-process ephemeral UUID for forensics.
    strategy_name: the normalized display/config name used to resolve the
        deployment identity.
    migrated: True if `SQLiteStore.backfill_deployment_id` rewrote rows from
        the old bare-name identity to the new deployment_id.
    """

    deployment_id: str
    run_id: str
    strategy_name: str
    migrated: bool


@dataclass(frozen=True)
class ResumeInfo:
    """Output of `_detect_state_resume`.

    is_resume: True iff a row exists in `strategy_state` for this deployment.
    version: the state_version column from the matching row (None if no row).
    state_keys: keys of the deserialized state_data JSON (empty list if row
        exists but state_data failed to parse).

    Field names preserve what `_print_startup_banner` reads today — the
    `existing_state_info` dict had keys {"version", "keys"}, and the helper
    surfaces the same information on a typed object.
    """

    is_resume: bool
    version: int | None
    state_keys: list[str]


@dataclass(frozen=True)
class StrategyBootstrap:
    """Strategy discovery + config loading outputs needed later in `run()`."""

    strategy_class: Any
    strategy_name: str
    strategy_config: dict[str, Any]
    multi_chain: bool
    config_file: str | None
    normalized_copy_mode: str | None
    strategy_chains: list[str]
    strategy_protocols: dict[str, list[str]]
    config_display_name: str
    effective_dry_run: bool


@dataclass(frozen=True)
class RuntimeBootstrap:
    """Resolved runtime + identity outputs needed by startup and execution."""

    config_chain: str | None
    resolved_network: str
    runtime_config: Any
    chain_wallets: Any
    strategy_id: str
    run_id: str


@dataclass
class ComponentBundle:
    """Built components that single-run and continuous execution need.

    Populated by `_build_components`. Runtime-handle fields typed
    as ``Any = None`` because their concrete types pull in heavy imports and
    the helper boundary avoids the circular-import risk that would arise from
    typing (e.g.) `StrategyRunner` at module load time.
    """

    runner: Any = None  # StrategyRunner
    state_manager: Any = None  # GatewayStateManager
    execution_orchestrator: Any = None
    price_oracle: Any = None
    balance_provider: Any = None
    ohlcv_provider: Any = None
    solana_fork_mgr: Any = None  # optional, anvil+Solana only
    cleanup_fn: Any = None  # Callable[[], Awaitable[None]] — untyped to avoid import cycles

    # Copy-trading (optional, only if copy_trading in config)
    copy_signal_engine: Any = None
    copy_ledger: Any = None
    copy_replay_runner: Any = None

    # Other runtime handles strategies attach
    circuit_breaker: Any = None
    stuck_detector: Any = None
    emergency_manager: Any = None
    operator_card_generator: Any = None
