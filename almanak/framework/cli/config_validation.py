"""One config-validation engine shared by ``strat check`` and runner boot (VIB-5986).

Why this module exists: a hosted deployment ran for 3+ days in permanent
fail-closed HOLD because its config shipped placeholders
(``morpho_market_id: ""``, ``morpho_lltv_bps: 0``) and nothing between the
author and the pod enforced the config contract. Two structural gaps made
that possible:

1. **Validation-path drift** — ``strat check`` validated the *raw file*
   config while the runtime deep-merges the hosted
   ``ALMANAK_STRATEGY_CONFIG`` env override on top of it
   (``run.load_strategy_config``). "check passed, deployment invalid" was
   possible by construction.
2. **No structural placeholder detection for identity keys** — the
   placeholder scanner deliberately skips empty strings, so an empty market
   id sailed through.

This module is the single composition point both surfaces consume:

* :func:`load_effective_config` — parse + schema-validate the config file
  and apply the EXACT hosted override merge the runtime applies, returning
  findings instead of raising so ``strat check`` can report everything in
  one pass. The primitives are the runtime's own
  (``run.parse_strategy_config_file`` / ``run._apply_env_strategy_config_override``),
  so check and runtime cannot drift.
* :func:`config_model_findings` / :func:`enforce_config_model` — the
  optional per-strategy Pydantic contract (``CONFIG_MODEL`` class attribute,
  see :class:`~almanak.framework.strategies.intent_strategy.IntentStrategy`).
  ``strat check`` consumes the findings form; runner boot consumes the
  raising form (``ConfigValidationError``) via
  ``_strategy_config.coerce_strategy_config``.
* :func:`scan_market_identity_findings` — the narrow structural heuristic
  for the incident class: a *present but empty* ``*_market_id`` key is never
  a valid configuration (authors who don't use the field omit it), so it is
  an ERROR; a zero LLTV-style risk parameter is a WARNING.

Semantic rule (blueprint 04 / VIB-5986): **missing or invalid config is a
boot failure; HOLD is for runtime conditions only.** ``validate_config()``
raising ``ConfigValidationError`` stays the semantic authority for
cross-field invariants; ``CONFIG_MODEL`` covers shape/required-key checks
declaratively so generated strategies get them for free.

Module-level imports stay stdlib-only: ``_strategy_config`` (which must
remain lightweight) lazily imports this module, and the run.py primitives
are themselves imported lazily here to keep the dependency graph acyclic.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Stable reason-code prefixes for boot-failure lifecycle writes. The hosted
# platform keys off these strings (WriteAgentStateRequest.error_message), so
# treat them as a wire contract: never rename, only add.
CONFIG_VALIDATION_FAILED = "CONFIG_VALIDATION_FAILED"
STRATEGY_INIT_FAILED = "STRATEGY_INIT_FAILED"

# Finding codes (stable slugs for --json consumers).
CODE_CONFIG_PARSE_ERROR = "config_parse_error"
CODE_CONFIG_OVERRIDE_INVALID = "config_override_invalid"
CODE_CONFIG_MODEL_VIOLATION = "config_model_violation"
CODE_EMPTY_MARKET_IDENTITY = "empty_market_identity"
CODE_ZERO_RISK_PARAMETER = "zero_risk_parameter"


@dataclass(frozen=True)
class ConfigFinding:
    """A single engine finding, deliberately harness-neutral.

    ``strat check`` maps these into its own ``Finding`` (adding layer /
    file context); the runner boot path only ever consumes the raising
    form. ``severity`` is ``"error"`` or ``"warning"``.
    """

    severity: str
    code: str
    message: str
    field: str | None = None


# =============================================================================
# Effective-config loading (file parse + hosted env override merge)
# =============================================================================


def load_effective_config(
    strategy_dir: Path,
) -> tuple[dict[str, Any] | None, Path | None, list[ConfigFinding]]:
    """Load the *effective* strategy config exactly as the runtime would.

    Finds ``config.json`` / ``config.yaml`` / ``config.yml`` in
    ``strategy_dir`` (same precedence as ``run.load_strategy_config``),
    parses + schema-validates it through ``run.parse_strategy_config_file``,
    then deep-merges the hosted ``ALMANAK_STRATEGY_CONFIG`` env override via
    ``run._apply_env_strategy_config_override`` — the same two primitives
    the runner boot path composes, so a config that passes here is the
    config the strategy actually runs with.

    Returns ``(effective_config, config_path, findings)``. A missing config
    file is not an error: ``(None, None, [])`` — unless the hosted env
    override is present, in which case the override alone forms the
    effective config (mirroring ``load_strategy_config``'s behaviour for
    hosted runs without a checked-in file).

    Parse / schema / override failures are returned as ERROR findings
    instead of raising, so ``strat check`` can keep reporting its other
    layers in the same invocation.
    """
    import click

    from .run import _apply_env_strategy_config_override, parse_strategy_config_file

    findings: list[ConfigFinding] = []
    config: dict[str, Any] | None = None
    config_path: Path | None = None

    for name in ("config.json", "config.yaml", "config.yml"):
        path = strategy_dir / name
        if not path.exists():
            continue
        config_path = path
        try:
            config = parse_strategy_config_file(path, warn_unknown_keys=False)
        except click.ClickException as exc:
            findings.append(
                ConfigFinding(
                    severity="error",
                    code=CODE_CONFIG_PARSE_ERROR,
                    message=exc.message,
                )
            )
            return None, config_path, findings
        break

    try:
        effective = _apply_env_strategy_config_override(config if config is not None else {}, echo=False)
    except click.ClickException as exc:
        findings.append(
            ConfigFinding(
                severity="error",
                code=CODE_CONFIG_OVERRIDE_INVALID,
                message=exc.message,
            )
        )
        # The file-loaded config is still useful for the remaining layers.
        return config, config_path, findings

    if config is None and not effective:
        # No file and no hosted override — genuinely no config.
        return None, None, findings

    return effective, config_path, findings


# =============================================================================
# CONFIG_MODEL — optional per-strategy Pydantic contract
# =============================================================================


def _framework_owned_keys() -> set[str]:
    """Config keys the framework consumes; stripped before CONFIG_MODEL validation.

    A strategy's ``CONFIG_MODEL`` declares the strategy's *own* keys.
    Framework-level keys (``chain``, ``token_funding``, runtime-injected
    identity, meta keys) must not require re-declaration in every model, so
    they are filtered out of the payload — UNLESS the model explicitly
    declares one, in which case the declaration wins and the key flows
    through (see :func:`_config_model_payload`).
    """
    from almanak.config.strategy import StrategyConfig

    from ._strategy_config import _FRAMEWORK_META_KEYS, _RUNTIME_FIELDS

    return set(StrategyConfig.model_fields) | _RUNTIME_FIELDS | _FRAMEWORK_META_KEYS


def _config_model_payload(model: type, config: Mapping[str, Any]) -> dict[str, Any]:
    """Project ``config`` onto the keys ``CONFIG_MODEL`` should see."""
    declared = set(getattr(model, "model_fields", {}) or {})
    stripped = _framework_owned_keys() - declared
    return {k: v for k, v in config.items() if k not in stripped}


def config_model_findings(strategy_class: type, config: Mapping[str, Any] | None) -> list[ConfigFinding]:
    """Validate ``config`` against the strategy's optional ``CONFIG_MODEL``.

    Returns one ERROR finding per Pydantic violation (missing required key,
    type mismatch, unknown key under ``extra="forbid"``, failed field
    validator). Strategies without a ``CONFIG_MODEL`` return ``[]`` — the
    contract is opt-in and legacy dict/dataclass strategies are unaffected.

    A missing config (``None``) is validated as an empty mapping: a declared
    contract with required fields must flag "no config at all" — otherwise
    ``strat check`` (no file → ``None``) would pass a deployment the runner
    (which always materialises a dict) refuses to boot, reintroducing the
    check/runtime drift this engine exists to close.
    """
    model = getattr(strategy_class, "CONFIG_MODEL", None)
    if model is None:
        return []
    if not isinstance(config, Mapping):
        config = {}

    try:
        from pydantic import BaseModel, ValidationError
    except ImportError:  # pragma: no cover - pydantic is a hard SDK dep
        return []

    if not (isinstance(model, type) and issubclass(model, BaseModel)):
        return [
            ConfigFinding(
                severity="error",
                code=CODE_CONFIG_MODEL_VIOLATION,
                message=(
                    f"{strategy_class.__name__}.CONFIG_MODEL must be a Pydantic v2 BaseModel subclass, got {model!r}."
                ),
            )
        ]

    try:
        model.model_validate(_config_model_payload(model, config))
    except ValidationError as exc:
        return [
            ConfigFinding(
                severity="error",
                code=CODE_CONFIG_MODEL_VIOLATION,
                message=err.get("msg", "invalid value"),
                field=".".join(str(part) for part in err.get("loc", ())) or None,
            )
            for err in exc.errors()
        ]
    return []


def enforce_config_model(strategy_class: type, config: Mapping[str, Any] | None) -> None:
    """Raising form of :func:`config_model_findings` for the boot path.

    Raises ``ConfigValidationError`` (the same exception
    ``validate_config()`` uses, so both contracts share one failure channel)
    carrying every violation in one message. Called from
    ``_strategy_config.coerce_strategy_config`` so ``strat run`` and
    ``strat backtest`` enforce identically.
    """
    findings = config_model_findings(strategy_class, config)
    errors = [f for f in findings if f.severity == "error"]
    if not errors:
        return

    from ..strategies.exceptions import ConfigValidationError

    detail = "; ".join(f"{f.field or '<config>'}: {f.message}" for f in errors)
    raise ConfigValidationError(
        f"CONFIG_MODEL validation failed ({len(errors)} violation(s)): {detail}",
        field=errors[0].field,
    )


# =============================================================================
# Structural placeholder scan for market-identity keys (the incident class)
# =============================================================================


def _is_market_identity_key(key: str) -> bool:
    """``market_id`` / ``*_market_id`` keys, excluding ``_``-prefixed comment keys.

    Config authors use ``_comment_market_id``-style keys as inline docs
    (JSON has no comments); those are never read by strategies and must not
    be flagged.
    """
    if not isinstance(key, str) or key.startswith("_"):
        return False
    return key == "market_id" or key.endswith("_market_id")


def _is_zero_lltv_key(key: str, value: Any) -> bool:
    """A zero LLTV-style parameter is protocol-impossible (real LLTVs are > 0)."""
    if not isinstance(key, str) or key.startswith("_") or "lltv" not in key.lower():
        return False
    return isinstance(value, int | float) and not isinstance(value, bool) and value == 0


def scan_market_identity_findings(config: Mapping[str, Any] | None) -> list[ConfigFinding]:
    """Flag present-but-empty market identity keys (recursive walk).

    A market id is an *identity*: strategies that don't use the field omit
    the key, so ``""`` / ``null`` can only mean "unresolved placeholder" —
    the exact shape that shipped the 3-day fail-closed HOLD incident. ERROR,
    not WARNING, because a strategy keyed to an empty market id can never
    trade. Zero LLTV-style values are WARNING (name heuristic, kept narrow
    per the no-global-placeholder-rules decision in VIB-5986).
    """
    findings: list[ConfigFinding] = []
    if not isinstance(config, Mapping):
        return findings

    def _walk(obj: Any, path: str) -> None:
        if isinstance(obj, Mapping):
            for key, value in obj.items():
                key_path = f"{path}.{key}" if path else str(key)
                if _is_market_identity_key(str(key)) and (
                    value is None or (isinstance(value, str) and not value.strip())
                ):
                    findings.append(
                        ConfigFinding(
                            severity="error",
                            code=CODE_EMPTY_MARKET_IDENTITY,
                            message=(
                                f"Config key '{key_path}' is present but empty. A market id is an "
                                "identity key — resolve the real market (e.g. `almanak ax "
                                "lending-reserves --protocol morpho_blue`) and pin it, or remove "
                                "the key entirely if this strategy does not use it."
                            ),
                            field=key_path,
                        )
                    )
                elif _is_zero_lltv_key(str(key), value):
                    findings.append(
                        ConfigFinding(
                            severity="warning",
                            code=CODE_ZERO_RISK_PARAMETER,
                            message=(
                                f"Config key '{key_path}' is 0. LLTV-style risk parameters are "
                                "never zero on real markets — this looks like an unresolved "
                                "placeholder."
                            ),
                            field=key_path,
                        )
                    )
                else:
                    _walk(value, key_path)
        elif isinstance(obj, list):
            for idx, value in enumerate(obj):
                _walk(value, f"{path}[{idx}]")

    _walk(config, "")
    return findings


__all__ = [
    "CODE_CONFIG_MODEL_VIOLATION",
    "CODE_CONFIG_OVERRIDE_INVALID",
    "CODE_CONFIG_PARSE_ERROR",
    "CODE_EMPTY_MARKET_IDENTITY",
    "CODE_ZERO_RISK_PARAMETER",
    "CONFIG_VALIDATION_FAILED",
    "STRATEGY_INIT_FAILED",
    "ConfigFinding",
    "config_model_findings",
    "enforce_config_model",
    "load_effective_config",
    "scan_market_identity_findings",
]
