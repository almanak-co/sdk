"""Connector discovery and self-registration metadata.

A concrete connector publishes one lightweight ``CONNECTOR`` object from
``almanak/connectors/<name>/connector.py``. Central registries consume those
connector objects instead of importing connector-specific provider modules by
hand.

The connector object is metadata plus lazy import references. It must stay
strategy-safe: import strings may point at gateway-side modules, but this
module never imports those targets unless a caller explicitly asks it to.
"""

from __future__ import annotations

import importlib
import importlib.util
import pkgutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "CONNECTOR_REGISTRY",
    "CONNECTOR_DESCRIPTOR_REGISTRY",
    "Connector",
    "ConnectorDescriptor",
    "ConnectorRegistry",
    "ConnectorDescriptorRegistry",
    "ConnectorDiscoveryError",
    "ImportRef",
]


class ConnectorDiscoveryError(Exception):
    """Connector discovery or validation failed."""


@dataclass(frozen=True)
class ImportRef:
    """Lazy reference to a class or object.

    ``module`` must be an absolute module path. Keeping import targets as
    strings lets connectors mention gateway-side providers without
    pulling gateway-only code into the strategy import graph.
    """

    module: str
    attribute: str
    order: int | None = None

    def __post_init__(self) -> None:
        """Validate the lazy import reference without importing the target."""
        if not isinstance(self.module, str) or not self.module.strip():
            raise ValueError(f"ImportRef.module must be a non-empty string, got {self.module!r}")
        if self.module.startswith("."):
            raise ValueError(f"ImportRef.module must be absolute, got {self.module!r}")
        if not isinstance(self.attribute, str) or not self.attribute.strip():
            raise ValueError(f"ImportRef.attribute must be a non-empty string, got {self.attribute!r}")
        if self.order is not None and (not isinstance(self.order, int) or self.order < 0):
            raise ValueError(f"ImportRef.order must be None or a non-negative int, got {self.order!r}")

    def load(self) -> Any:
        """Import and return the referenced attribute."""
        module = importlib.import_module(self.module)
        try:
            return getattr(module, self.attribute)
        except AttributeError as exc:
            raise ConnectorDiscoveryError(f"{self.module!r} does not define attribute {self.attribute!r}") from exc

    def instantiate(self, *args: Any, **kwargs: Any) -> Any:
        """Import the referenced callable and instantiate/call it."""
        target = self.load()
        return target(*args, **kwargs)


@dataclass(frozen=True)
class Connector:
    """Lightweight connector-owned capability manifest.

    The connector manifest intentionally starts small. New capability references can
    be added as central registries migrate to descriptor-backed discovery.
    """

    name: str
    kind: ProtocolKind
    aliases: tuple[str, ...] = field(default_factory=tuple)
    receipt_parser_protocols: tuple[str, ...] | None = None
    receipt_parser_connector: ImportRef | None = None
    gas_estimate_connector: ImportRef | None = None
    agent_read_connector: ImportRef | None = None
    agent_read_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)
    vault_tool_connector: ImportRef | None = None
    vault_tool_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)
    gateway_connector: ImportRef | None = None
    gateway_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        """Validate connector-owned manifest metadata."""
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(f"Connector.name must be a non-empty string, got {self.name!r}")
        if not isinstance(self.kind, ProtocolKind):
            raise ValueError(f"Connector.kind must be a ProtocolKind, got {self.kind!r}")
        if not isinstance(self.aliases, tuple):
            raise ValueError(f"Connector.aliases must be a tuple[str, ...], got {self.aliases!r}")
        bad_aliases = [a for a in self.aliases if not isinstance(a, str) or not a.strip()]
        if bad_aliases:
            raise ValueError(f"Connector.aliases must contain only non-empty strings, got {bad_aliases!r}")
        if len(set(self.aliases)) != len(self.aliases):
            raise ValueError(f"Connector.aliases contains duplicates: {self.aliases!r}")
        if self.name in self.aliases:
            raise ValueError(f"Connector.aliases must not include canonical name {self.name!r}")
        self._validate_gateway_connectors()
        self._validate_receipt_parser_protocols()
        self._validate_gas_estimate_connector()
        self._validate_agent_read_connectors()
        self._validate_vault_tool_connectors()

    def _validate_gateway_connectors(self) -> None:
        """Validate gateway provider import references and ordering keys."""
        if self.gateway_connector is not None and not isinstance(self.gateway_connector, ImportRef):
            raise ValueError(
                f"Connector.gateway_connector must be None or an ImportRef, got {self.gateway_connector!r}"
            )
        if not isinstance(self.gateway_connectors, tuple):
            raise ValueError(
                f"Connector.gateway_connectors must be a tuple[ImportRef, ...], got {self.gateway_connectors!r}"
            )
        bad_refs = [ref for ref in self.gateway_connectors if not isinstance(ref, ImportRef)]
        if bad_refs:
            raise ValueError(f"Connector.gateway_connectors must contain only ImportRef values, got {bad_refs!r}")
        ref_keys = [(ref.module, ref.attribute) for ref in self.gateway_connector_refs]
        if len(set(ref_keys)) != len(ref_keys):
            raise ValueError(f"Connector gateway connector refs contain duplicates: {ref_keys!r}")

    def _validate_receipt_parser_protocols(self) -> None:
        """Validate receipt-parser import references and advertised protocol keys."""
        if self.receipt_parser_connector is not None and not isinstance(self.receipt_parser_connector, ImportRef):
            raise ValueError(
                "Connector.receipt_parser_connector must be None or an ImportRef, "
                f"got {self.receipt_parser_connector!r}"
            )
        if self.receipt_parser_protocols is None:
            return
        if self.receipt_parser_connector is None:
            raise ValueError(
                "Connector.receipt_parser_protocols may only be set when receipt_parser_connector is also set"
            )
        if not isinstance(self.receipt_parser_protocols, tuple) or not self.receipt_parser_protocols:
            raise ValueError(
                "Connector.receipt_parser_protocols must be None or a non-empty tuple[str, ...], "
                f"got {self.receipt_parser_protocols!r}"
            )
        bad_protocols = [
            protocol
            for protocol in self.receipt_parser_protocols
            if not isinstance(protocol, str) or not protocol.strip()
        ]
        if bad_protocols:
            raise ValueError(
                f"Connector.receipt_parser_protocols must contain only non-empty strings, got {bad_protocols!r}"
            )
        if len(set(self.receipt_parser_protocols)) != len(self.receipt_parser_protocols):
            raise ValueError(
                f"Connector.receipt_parser_protocols contains duplicates: {self.receipt_parser_protocols!r}"
            )

    def _validate_gas_estimate_connector(self) -> None:
        """Validate the strategy-side gas-estimate provider import reference."""
        if self.gas_estimate_connector is not None and not isinstance(self.gas_estimate_connector, ImportRef):
            raise ValueError(
                f"Connector.gas_estimate_connector must be None or an ImportRef, got {self.gas_estimate_connector!r}"
            )

    def _validate_agent_read_connectors(self) -> None:
        """Validate agent-read provider import references."""
        if self.agent_read_connector is not None and not isinstance(self.agent_read_connector, ImportRef):
            raise ValueError(
                f"Connector.agent_read_connector must be None or an ImportRef, got {self.agent_read_connector!r}"
            )
        if not isinstance(self.agent_read_connectors, tuple):
            raise ValueError(
                f"Connector.agent_read_connectors must be a tuple[ImportRef, ...], got {self.agent_read_connectors!r}"
            )
        bad_refs = [ref for ref in self.agent_read_connectors if not isinstance(ref, ImportRef)]
        if bad_refs:
            raise ValueError(f"Connector.agent_read_connectors must contain only ImportRef values, got {bad_refs!r}")
        ref_keys = [(ref.module, ref.attribute) for ref in self.agent_read_connector_refs]
        if len(set(ref_keys)) != len(ref_keys):
            raise ValueError(f"Connector agent-read connector refs contain duplicates: {ref_keys!r}")

    def _validate_vault_tool_connectors(self) -> None:
        """Validate vault-tool provider import references."""
        if self.vault_tool_connector is not None and not isinstance(self.vault_tool_connector, ImportRef):
            raise ValueError(
                f"Connector.vault_tool_connector must be None or an ImportRef, got {self.vault_tool_connector!r}"
            )
        if not isinstance(self.vault_tool_connectors, tuple):
            raise ValueError(
                f"Connector.vault_tool_connectors must be a tuple[ImportRef, ...], got {self.vault_tool_connectors!r}"
            )
        bad_refs = [ref for ref in self.vault_tool_connectors if not isinstance(ref, ImportRef)]
        if bad_refs:
            raise ValueError(f"Connector.vault_tool_connectors must contain only ImportRef values, got {bad_refs!r}")
        ref_keys = [(ref.module, ref.attribute) for ref in self.vault_tool_connector_refs]
        if len(set(ref_keys)) != len(ref_keys):
            raise ValueError(f"Connector vault-tool connector refs contain duplicates: {ref_keys!r}")

    @property
    def protocol(self) -> ProtocolName:
        """Canonical protocol name as the registry key type."""
        return ProtocolName(self.name)

    @property
    def protocol_keys(self) -> frozenset[str]:
        """Canonical name plus aliases."""
        return frozenset((self.name, *self.aliases))

    @property
    def receipt_parser_keys(self) -> frozenset[str]:
        """Protocol keys the receipt-parser provider publishes.

        Defaults to the connector identity keys. Connectors whose folder name
        differs from their receipt-parser protocol key can set
        ``receipt_parser_protocols`` explicitly.
        """
        if self.receipt_parser_protocols is None:
            return self.protocol_keys
        return frozenset(self.receipt_parser_protocols)

    @property
    def gateway_connector_refs(self) -> tuple[ImportRef, ...]:
        """Gateway-side provider import refs owned by this connector.

        Most connectors publish a single gateway provider. Fork-style
        protocols can publish additional provider refs from the owning folder;
        for example, the Uniswap V3 folder owns the Agni Finance gateway
        address provider because Agni reuses the V3 connector surface.
        """
        if self.gateway_connector is None:
            return self.gateway_connectors
        return (self.gateway_connector, *self.gateway_connectors)

    @property
    def agent_read_connector_refs(self) -> tuple[ImportRef, ...]:
        """Agent-tool read-descriptor provider import refs owned by this connector."""
        if self.agent_read_connector is None:
            return self.agent_read_connectors
        return (self.agent_read_connector, *self.agent_read_connectors)

    @property
    def vault_tool_connector_refs(self) -> tuple[ImportRef, ...]:
        """Vault-tool provider import refs owned by this connector."""
        if self.vault_tool_connector is None:
            return self.vault_tool_connectors
        return (self.vault_tool_connector, *self.vault_tool_connectors)

    @property
    def discovery_keys(self) -> frozenset[str]:
        """All keys that should resolve to this connector."""
        keys = set(self.protocol_keys)
        if self.receipt_parser_connector is not None:
            keys.update(self.receipt_parser_keys)
        return frozenset(keys)


class ConnectorRegistry:
    """Discover connector-owned ``CONNECTOR`` objects.

    Discovery scans only first-level connector packages and imports
    ``almanak/connectors/<name>/connector.py`` when it exists. Missing connector
    manifests are ignored so the migration can proceed one connector at a time.
    """

    def __init__(self, package_name: str = "almanak.connectors") -> None:
        """Create a registry for connector manifests under ``package_name``."""
        self._package_name = package_name
        self._connectors: tuple[Connector, ...] | None = None
        self._discovering = False

    def all(self) -> tuple[Connector, ...]:
        """Return every discovered connector sorted by connector name."""
        if self._connectors is None:
            if self._discovering:
                raise ConnectorDiscoveryError(
                    "ConnectorRegistry.all() detected recursive connector discovery; "
                    "calling connector registry discovery during manifest import is disallowed."
                )
            self._discovering = True
            try:
                self._connectors = self._discover()
            finally:
                self._discovering = False
        return self._connectors

    def get(self, name: str) -> Connector | None:
        """Return the connector for ``name`` or any published connector key."""
        for connector in self.all():
            if name in connector.discovery_keys:
                return connector
        return None

    def with_receipt_parser(self) -> tuple[Connector, ...]:
        """Return connectors that publish a receipt-parser connector."""
        return tuple(d for d in self.all() if d.receipt_parser_connector is not None)

    def with_gas_estimate(self) -> tuple[Connector, ...]:
        """Return connectors that publish a gas-estimate connector."""
        return tuple(d for d in self.all() if d.gas_estimate_connector is not None)

    def with_agent_read(self) -> tuple[Connector, ...]:
        """Return connectors that publish agent-read connectors."""
        return tuple(d for d in self.all() if d.agent_read_connector_refs)

    def with_vault_tool(self) -> tuple[Connector, ...]:
        """Return connectors that publish vault-tool connectors."""
        return tuple(d for d in self.all() if d.vault_tool_connector_refs)

    def clear(self) -> None:
        """Test helper: clear the discovery cache."""
        self._connectors = None
        self._discovering = False

    def _discover(self) -> tuple[Connector, ...]:
        """Scan connector packages and validate discovered manifest ownership."""
        package = importlib.import_module(self._package_name)
        connectors: list[Connector] = []
        seen_names: set[str] = set()
        seen_keys: dict[str, str] = {}
        seen_gateway_orders: dict[int, str] = {}

        for info in pkgutil.iter_modules(package.__path__):
            if not info.ispkg or info.name.startswith("_"):
                continue
            connector = self._load_connector(info.name)
            if connector is None:
                continue
            self._validate_connector_owner(info.name, connector)
            if connector.name in seen_names:
                raise ConnectorDiscoveryError(f"Connector {connector.name!r} discovered twice")
            seen_names.add(connector.name)
            for key in connector.discovery_keys:
                owner = seen_keys.get(key)
                if owner is not None:
                    raise ConnectorDiscoveryError(
                        f"Connector key {key!r} is claimed by both {owner!r} and {connector.name!r}"
                    )
                seen_keys[key] = connector.name
            for gateway_ref in connector.gateway_connector_refs:
                if gateway_ref.order is None:
                    continue
                owner = seen_gateway_orders.get(gateway_ref.order)
                if owner is not None:
                    raise ConnectorDiscoveryError(
                        f"Gateway connector order {gateway_ref.order} is claimed by both "
                        f"{owner!r} and {connector.name!r}"
                    )
                seen_gateway_orders[gateway_ref.order] = connector.name
            connectors.append(connector)

        return tuple(sorted(connectors, key=lambda d: d.name))

    def _load_connector(self, connector_name: str) -> Connector | None:
        """Load ``CONNECTOR`` from one connector package if its manifest exists."""
        connector_path = self._connector_file(connector_name)
        if connector_path is None:
            return None
        module_name = f"{self._package_name}.{connector_name}.connector"
        spec = importlib.util.spec_from_file_location(module_name, connector_path)
        if spec is None or spec.loader is None:
            raise ConnectorDiscoveryError(f"Could not load connector manifest {connector_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        connector = getattr(module, "CONNECTOR", None)
        if connector is None:
            raise ConnectorDiscoveryError(f"{module_name} must define CONNECTOR")
        if not isinstance(connector, Connector):
            raise ConnectorDiscoveryError(
                f"{module_name}.CONNECTOR must be a Connector, got {type(connector).__qualname__}"
            )
        return connector

    def _connector_file(self, connector_name: str) -> Path | None:
        """Return a connector manifest path without importing the connector package."""
        package = importlib.import_module(self._package_name)
        for package_path in package.__path__:
            connector_path = Path(package_path) / connector_name / "connector.py"
            if connector_path.is_file():
                return connector_path
        return None

    @staticmethod
    def _validate_connector_owner(connector_name: str, connector: Connector) -> None:
        """Require a connector manifest to declare the folder-owned name."""
        if connector.name != connector_name:
            raise ConnectorDiscoveryError(
                f"Connector in folder {connector_name!r} declares name {connector.name!r}; "
                "connector.name must match the connector folder"
            )


ConnectorDescriptor = Connector
ConnectorDescriptorRegistry = ConnectorRegistry

CONNECTOR_REGISTRY = ConnectorRegistry()
CONNECTOR_DESCRIPTOR_REGISTRY = CONNECTOR_REGISTRY
