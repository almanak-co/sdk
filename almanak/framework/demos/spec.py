"""Typed demo spec — unifies decorator metadata, config.json, sidecar entry.

A ``DemoSpec`` is the in-memory composition of three existing sources of
truth. It is **not** persisted: ``DemoCatalog.discover()`` rebuilds the
list from disk on every call. The trade-off is intentional — staleness
is the failure mode we are trying to eliminate.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import logging
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from almanak.framework.strategies.metadata import StrategyMetadata

from .sidecar import SidecarEntry, SidecarRegistry

logger = logging.getLogger(__name__)


def default_demos_root() -> Path:
    """Resolve the canonical ``almanak/demo_strategies`` directory.

    Walks up from this file to locate the ``almanak`` package root, then
    returns ``<package_root>/demo_strategies``. Falls back to a parent-relative
    path so the function still works when invoked from an installed wheel.
    """
    here = Path(__file__).resolve()
    # almanak/framework/demos/spec.py -> almanak/demo_strategies
    return here.parent.parent.parent / "demo_strategies"


@dataclass(frozen=True)
class QaConfig:
    """Optional QA-only configuration nested under ``config.json["qa"]``.

    These fields exist for the smoke harness, CI gates, and the QA-100
    spreadsheet. They are **never** read by the runtime CLI path so a demo
    without a ``qa`` block runs identically to one with it.

    Attributes:
        regress: lanes this demo should appear in (``smoke``, ``nightly``,
            ``mainnet``). Empty list means "no automatic regression run".
        force_action: optional override for action selection during testing
            (e.g. ``"open"``, ``"supply"``). Mirrors the connector demo's
            ``force_action`` semantics for the sidecar harness.
        expected_actions: number of on-chain actions expected per iteration
            (used by the QA-100 sheet to drive "tight params").
        sidecar_skip_ticket: if set, the demo opts out of sidecar-coverage
            gate enforcement. The Linear ticket is required so the skip
            cannot be a silent regression-coverage hole.
    """

    regress: tuple[str, ...] = ()
    force_action: str | None = None
    expected_actions: int | None = None
    sidecar_skip_ticket: str | None = None

    @classmethod
    def from_mapping(cls, raw: Any) -> QaConfig:
        """Parse a ``config.json["qa"]`` block. Tolerant of missing/empty inputs."""
        if not isinstance(raw, dict):
            return cls()
        regress = raw.get("regress", ())
        if isinstance(regress, str):
            regress = (regress,)
        elif isinstance(regress, list):
            regress = tuple(str(x) for x in regress)
        else:
            regress = ()
        force_action = raw.get("force_action")
        if force_action is not None and not isinstance(force_action, str):
            force_action = str(force_action)
        expected_actions = raw.get("expected_actions")
        if expected_actions is not None:
            try:
                expected_actions = int(expected_actions)
            except (TypeError, ValueError):
                expected_actions = None
        sidecar_skip_ticket = raw.get("sidecar_skip_ticket")
        if sidecar_skip_ticket is not None and not isinstance(sidecar_skip_ticket, str):
            sidecar_skip_ticket = str(sidecar_skip_ticket)
        return cls(
            regress=regress,
            force_action=force_action,
            expected_actions=expected_actions,
            sidecar_skip_ticket=sidecar_skip_ticket,
        )


@dataclass(frozen=True)
class DemoLoadError:
    """A demo directory that failed to load.

    Surfaced by ``DemoCatalog.discover()`` instead of raised so a single
    broken demo cannot poison the whole catalog. CI gate 1 reports these.
    """

    directory: Path
    reason: str


@dataclass(frozen=True)
class DemoSpec:
    """Composed view of one demo strategy.

    Construction is via ``DemoSpec.load(directory)`` or
    ``DemoCatalog.discover()`` — never directly. ``metadata`` is the source
    of truth for chains/protocols/intent_types; ``config`` is the source of
    truth for runtime params and ``anvil_funding``; ``sidecar`` is non-None
    only for demos referenced from ``.github/sidecar-demos.yml``.

    ``name`` is the canonical CLI-facing identifier and **always** equals
    ``directory.name``. The decorator-derived ``metadata.name`` is preserved
    on ``metadata`` for telemetry/logging only — keying the catalog on
    ``directory.name`` keeps a single round-trippable key for
    ``almanak strat demo --name <slug>`` and ``scripts/run_demo.py
    --strategy <slug>`` (every other identifier on disk is the directory
    slug, so the CLI key must match).
    """

    name: str
    directory: Path
    metadata: StrategyMetadata
    config: dict[str, Any]
    qa: QaConfig
    sidecar: SidecarEntry | None

    @property
    def supported_chains(self) -> list[str]:
        return list(self.metadata.supported_chains)

    @property
    def default_chain(self) -> str:
        return self.metadata.default_chain or (
            self.metadata.supported_chains[0] if self.metadata.supported_chains else ""
        )

    @property
    def supported_protocols(self) -> list[str]:
        return list(self.metadata.supported_protocols)

    @property
    def intent_types(self) -> list[str]:
        return list(self.metadata.intent_types)

    @property
    def description(self) -> str:
        return self.metadata.description or self.config.get("description", "")

    def chains_in_config(self) -> list[str]:
        """Chains explicitly named in ``config.json`` (``chain`` or ``chains``)."""
        chains_val = self.config.get("chains")
        if isinstance(chains_val, list):
            return [str(c) for c in chains_val if isinstance(c, str | int)]
        chain_val = self.config.get("chain")
        if isinstance(chain_val, str):
            return [chain_val]
        return []

    def required_funding(self, chain: str | None = None) -> dict[str, Decimal]:
        """Tokens the demo expects to be pre-funded for ``chain``.

        Reads ``config.json["anvil_funding"]``. Two layouts are supported:

        * Flat ``{token: amount}`` — applies to all chains the demo runs on.
        * Per-chain ``{chain: {token: amount}}`` — looked up by ``chain``,
          falling back to ``chain == default_chain``.

        Token amounts are returned as ``Decimal``. Zero or unparseable
        amounts are dropped.
        """
        funding = self.config.get("anvil_funding")
        if not isinstance(funding, dict) or not funding:
            return {}

        target_chain = chain or self.default_chain
        # Per-chain layout heuristic: if every value is itself a dict, treat
        # the outer dict's keys as chains.
        if funding and all(isinstance(v, dict) for v in funding.values()):
            chain_section = funding.get(target_chain) or {}
            return _coerce_funding(chain_section)
        return _coerce_funding(funding)

    @classmethod
    def load(cls, directory: Path, *, sidecar: SidecarEntry | None = None) -> DemoSpec:
        """Load a single demo dir. Raises ``DemoLoadError``-shaped errors.

        Discovery callers should prefer :meth:`DemoCatalog.discover` which
        catches load failures per-dir. Direct callers (e.g. unit tests)
        catch ``ValueError`` / ``OSError`` themselves.
        """
        if not directory.is_dir():
            raise FileNotFoundError(f"Demo directory does not exist: {directory}")

        strategy_file = directory / "strategy.py"
        if not strategy_file.is_file():
            raise FileNotFoundError(f"Demo {directory.name!r} is missing strategy.py — not a runnable demo")

        config_path = directory / "config.json"
        config: dict[str, Any] = {}
        if config_path.is_file():
            try:
                with open(config_path, encoding="utf-8") as fh:
                    parsed = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Malformed JSON in {config_path}: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"{config_path} must be a JSON object at the top level, got {type(parsed).__name__}")
            config = parsed

        metadata = _import_strategy_metadata(strategy_file)
        if metadata is None:
            raise ValueError(
                f"Demo {directory.name!r} has no @almanak_strategy decorator — "
                f"cannot derive supported_chains/protocols metadata"
            )

        qa = QaConfig.from_mapping(config.get("qa"))

        return cls(
            # Canonical CLI key — must match the directory slug so that
            # `DEMO_STRATEGY_NAMES` (a config.json glob over directories)
            # and the CLI selector (`almanak strat demo --name <slug>`)
            # agree. The decorator-derived metadata.name is retained on
            # `metadata` for telemetry; never use it as a lookup key.
            name=directory.name,
            # ``directory.resolve()`` so the path is absolute and free of
            # symlinks; ``DemoCatalog.by_directory`` and the sidecar lookup
            # both compare resolved paths and would otherwise miss matches
            # when the caller passes a relative or symlinked path.
            directory=directory.resolve(),
            metadata=metadata,
            config=config,
            qa=qa,
            sidecar=sidecar,
        )


@dataclass
class DemoCatalog:
    """Result of ``DemoSpec.discover()`` — successful specs + load errors."""

    specs: list[DemoSpec] = field(default_factory=list)
    errors: list[DemoLoadError] = field(default_factory=list)
    sidecar_registry: SidecarRegistry | None = None

    def __iter__(self):
        return iter(self.specs)

    def __len__(self) -> int:
        return len(self.specs)

    def by_name(self, name: str) -> DemoSpec | None:
        for spec in self.specs:
            if spec.name == name:
                return spec
        return None

    def by_directory(self, directory: Path) -> DemoSpec | None:
        directory = directory.resolve()
        for spec in self.specs:
            if spec.directory.resolve() == directory:
                return spec
        return None

    def for_chain(self, chain: str) -> list[DemoSpec]:
        return [s for s in self.specs if chain in s.supported_chains]

    def by_connector(self, connector: str) -> DemoSpec | None:
        if self.sidecar_registry is None:
            return None
        entry = self.sidecar_registry.connectors.get(connector)
        if entry is None:
            return None
        return self.by_directory(entry.demo_dir)

    @classmethod
    def discover(
        cls,
        root: Path | None = None,
        *,
        sidecar_registry: SidecarRegistry | None = None,
    ) -> DemoCatalog:
        """Walk ``root`` and load every directory containing ``strategy.py``.

        ``root`` defaults to :func:`default_demos_root`. Failed directories
        are recorded in :attr:`errors` rather than aborting the walk; the
        caller (e.g. CI gate 1) decides how to react.
        """
        root = (root or default_demos_root()).resolve()
        if sidecar_registry is None:
            try:
                sidecar_registry = SidecarRegistry.load_default()
            except FileNotFoundError:
                sidecar_registry = SidecarRegistry({})
            except (ValueError, RuntimeError, OSError) as exc:
                # Malformed or unreadable `.github/sidecar-demos.yml`
                # (missing demo_dir, bad max_iterations, permissions, etc.)
                # — degrade to "no sidecar info" rather than crashing
                # `almanak strat demo --list` for end users. CI gate 6
                # still validates the registry.
                logger.warning("Failed to load sidecar registry — proceeding without it: %s", exc)
                sidecar_registry = SidecarRegistry({})
        sidecar_by_dir = {entry.demo_dir.resolve(): entry for entry in sidecar_registry.entries()}

        catalog = cls(sidecar_registry=sidecar_registry)
        if not root.is_dir():
            catalog.errors.append(DemoLoadError(directory=root, reason=f"Demos root does not exist: {root}"))
            return catalog

        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name.startswith("_") or entry.name.startswith("."):
                continue
            if not (entry / "strategy.py").is_file():
                # Silent skip — directory may be a fixture / test scaffolding.
                # CI gate 1 catches dirs that look like demos (e.g. carry a
                # config.json) but lack a strategy.py.
                if (entry / "config.json").is_file():
                    catalog.errors.append(
                        DemoLoadError(
                            directory=entry,
                            reason="config.json present but strategy.py missing",
                        )
                    )
                continue
            try:
                spec = DemoSpec.load(
                    entry,
                    sidecar=sidecar_by_dir.get(entry.resolve()),
                )
            except Exception as exc:  # noqa: BLE001 - exec_module can raise anything
                # ``DemoSpec.load`` runs ``strategy.py`` via ``exec_module``;
                # the imported code can raise any exception (TypeError,
                # ImportError, SyntaxError, custom exceptions, …). One bad
                # demo must NOT poison the catalog — record it on
                # ``catalog.errors`` and continue. CI gate 1 surfaces these.
                logger.warning("Failed to load demo %s: %s", entry.name, exc)
                catalog.errors.append(DemoLoadError(directory=entry, reason=str(exc)))
                continue
            catalog.specs.append(spec)

        catalog.specs.sort(key=lambda s: s.name)
        return catalog


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _coerce_funding(raw: dict[str, Any]) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    for token, amount in raw.items():
        if not isinstance(token, str):
            continue
        try:
            value = Decimal(str(amount))
        except (InvalidOperation, TypeError, ValueError):
            logger.debug("Skipping non-decimal anvil_funding entry: %s=%r", token, amount)
            continue
        if value > 0:
            out[token] = value
    return out


def _strategy_module_name(strategy_file: Path) -> str:
    """Stable, collision-free module name for an ad-hoc strategy.py load."""
    digest = hashlib.sha1(str(strategy_file.resolve()).encode()).hexdigest()[:12]
    return f"_almanak_demos_spec_{strategy_file.parent.name}_{digest}"


def _import_strategy_metadata(strategy_file: Path) -> StrategyMetadata | None:
    """Import ``strategy.py`` and return the first ``STRATEGY_METADATA`` found.

    Cleans up after itself: removes the ad-hoc module from ``sys.modules``
    so repeated discovery doesn't accumulate state.
    """
    module_name = _strategy_module_name(strategy_file)
    if module_name in sys.modules:
        module = sys.modules[module_name]
    else:
        spec = importlib.util.spec_from_file_location(module_name, strategy_file)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise

    try:
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if not isinstance(obj, type):
                continue
            metadata = getattr(obj, "STRATEGY_METADATA", None)
            if isinstance(metadata, StrategyMetadata):
                return metadata
        return None
    finally:
        # Keep the module loaded if a parent import already cached it
        # (e.g. via ``almanak.framework.strategies`` auto-discovery), but
        # don't leak our ad-hoc one across discoveries.
        sys.modules.pop(module_name, None)
