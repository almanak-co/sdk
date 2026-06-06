"""Contract address registry for protocol contract addresses.

Maps known contract addresses per chain to protocol metadata and action
capabilities. Used by copy-trading signal decoding for fail-closed behavior.

W1 (VIB-4853): each protocol's per-chain address table is owned by its
connector folder (``almanak/connectors/<protocol>/addresses.py``). This
registry composes them through the strategy-side
:class:`almanak.connectors._strategy_base.address_registry.AddressRegistry`
— the single strategy-side seam that brokers every connector's address
table — rather than importing each connector ``addresses`` module by name.
The gateway-side ``GatewayAddressCapability`` interface mirrors the same
data for gateway callers and is gateway-only by import boundary (see
``tests/static/test_strategy_import_boundary.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec
from almanak.core.constants import resolve_chain_name


def _normalize_chain(chain: str) -> str:
    try:
        return resolve_chain_name(chain)
    except ValueError:
        return chain.strip().lower()


@dataclass
class ContractInfo:
    """Metadata about a known contract address."""

    protocol: str
    contract_type: str
    parser_module: str
    parser_class_name: str
    supported_actions: list[str] = field(default_factory=list)


class ContractRegistry:
    """Registry mapping (chain, address) pairs to ContractInfo."""

    def __init__(self) -> None:
        self._registry: dict[tuple[str, str], ContractInfo] = {}

    def register(self, chain: str, address: str, info: ContractInfo) -> None:
        """Register a contract address for a chain."""
        self._registry[(_normalize_chain(chain), address.lower())] = info

    def lookup(self, chain: str, address: str) -> ContractInfo | None:
        """Look up contract info by chain and address (case-insensitive)."""
        return self._registry.get((_normalize_chain(chain), address.lower()))

    def get_monitored_addresses(self, chain: str) -> list[str]:
        """Return all known contract addresses for a chain."""
        chain_key = _normalize_chain(chain)
        return [addr for (c, addr) in self._registry if c == chain_key]

    def get_supported_protocols(self) -> set[str]:
        """Return the set of all registered protocol names."""
        return {info.protocol for info in self._registry.values()}

    def is_action_supported(self, chain: str, address: str, action_type: str) -> bool:
        """Return whether an action is supported for a chain/address pair."""
        info = self.lookup(chain, address)
        if info is None:
            return False
        return action_type.upper() in {a.upper() for a in info.supported_actions}

    def capability_flags(self, chain: str, address: str, action_type: str) -> dict[str, bool]:
        """Return fail-closed capability flags for a target action."""
        chain_key = _normalize_chain(chain)
        chain_supported = any(c == chain_key for (c, _) in self._registry)
        info = self.lookup(chain, address)
        if info is None:
            return {
                "chain_supported": chain_supported,
                "protocol_supported": False,
                "action_supported": False,
            }

        return {
            "chain_supported": chain_supported,
            "protocol_supported": True,
            "action_supported": action_type.upper() in {a.upper() for a in info.supported_actions},
        }


def _connector_contract_monitoring_specs() -> tuple[ContractMonitoringSpec, ...]:
    """Load connector-published contract-monitoring specs from manifests."""
    specs: list[ContractMonitoringSpec] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_monitoring():
        if connector_manifest.contract_monitoring is None:
            continue
        loaded = connector_manifest.contract_monitoring.load()
        if isinstance(loaded, ContractMonitoringSpec):
            specs.append(loaded)
            continue
        if not isinstance(loaded, tuple) or not all(isinstance(spec, ContractMonitoringSpec) for spec in loaded):
            raise TypeError(
                f"{connector_manifest.contract_monitoring.module}.{connector_manifest.contract_monitoring.attribute} "
                "must be a ContractMonitoringSpec or tuple[ContractMonitoringSpec, ...]"
            )
        specs.extend(loaded)
    return tuple(specs)


def _register_contract_spec(registry: ContractRegistry, definition: ContractMonitoringSpec) -> None:
    """Register every address selected by one contract-monitoring spec."""
    matched_any = False
    for chain in AddressRegistry.address_supported_chains(definition.protocol):
        contracts = AddressRegistry.addresses_for(definition.protocol, chain)
        for contract_type, address in definition.matching_contracts(contracts):
            matched_any = True
            registry.register(
                chain,
                address,
                ContractInfo(
                    protocol=definition.protocol,
                    contract_type=contract_type,
                    parser_module=definition.parser_module,
                    parser_class_name=definition.parser_class_name,
                    supported_actions=list(definition.supported_actions),
                ),
            )
    if not matched_any:
        raise ValueError(
            f"ContractMonitoringSpec for protocol={definition.protocol!r} matched no addresses "
            f"(contract_key={definition.contract_key!r}, contract_key_prefix={definition.contract_key_prefix!r})"
        )


def get_default_registry() -> ContractRegistry:
    """Create a ContractRegistry populated from connector-owned address tables.

    Each protocol's per-chain address table is resolved through the
    strategy-side :class:`AddressRegistry` (W1 / VIB-4853), which brokers
    the connector's ``addresses.py``. The previous central registry at
    ``almanak.core.contracts`` has been deleted.
    """
    registry = ContractRegistry()
    for definition in _connector_contract_monitoring_specs():
        _register_contract_spec(registry, definition)
    return registry
