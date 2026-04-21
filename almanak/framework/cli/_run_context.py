"""Shared state carriers for the `almanak strat run` refactor (Phase 4b+).

`RunContext` accumulates state as `run()` progresses through its phases. The
frozen `IdentityInfo` and `ResumeInfo` dataclasses are the immutable outputs
of the identity-resolution and state-resume-detection helpers.

Gradual typing (the Phase 3c pattern): runtime-handle fields that carry
protocol-specific types live in `Any = None` slots. Typed fields get explicit
types. Fields are added incrementally as each helper lands — do NOT preload
fields for phases that haven't been extracted yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunContext:
    """Mutable state carrier threaded through `run()` phase helpers.

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
