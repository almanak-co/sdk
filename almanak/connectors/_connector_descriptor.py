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
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec

__all__ = [
    "CONNECTOR_REGISTRY",
    "CONNECTOR_DESCRIPTOR_REGISTRY",
    "CapabilitiesSpec",
    "Connector",
    "ConnectorDescriptor",
    "ConnectorRegistry",
    "ConnectorDescriptorRegistry",
    "ConnectorDiscoveryError",
    "ImportRef",
    "StrategyMatrixEntry",
    "SupportedChainsSpec",
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
class StrategyMatrixEntry:
    """Strategy support-matrix row declared from a connector manifest.

    Kept separate from ``_strategy_base.registry.MatrixEntry`` so descriptor
    discovery can stay strategy-safe and avoid importing framework intent
    vocabulary during connector manifest loading.
    """

    matrix_name: str
    category: str
    chains: frozenset[str]


@dataclass(frozen=True)
class Connector:
    """Lightweight connector-owned capability manifest.

    The connector manifest intentionally starts small. New capability references can
    be added as central registries migrate to descriptor-backed discovery.
    """

    name: str
    kind: ProtocolKind
    aliases: tuple[str, ...] = field(default_factory=tuple)
    address_tables: tuple[AddressTableSpec, ...] | None = None
    solana_programs: tuple[SolanaProgramSpec, ...] | None = None
    receipt_parser_protocols: tuple[str, ...] | None = None
    receipt_parser_connector: ImportRef | None = None
    gas_estimate_connector: ImportRef | None = None
    agent_read_connector: ImportRef | None = None
    agent_read_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)
    vault_tool_connector: ImportRef | None = None
    vault_tool_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)
    runner_hook_connector: ImportRef | None = None
    protocol_metadata: ImportRef | None = None
    principal_token_market_reader: ImportRef | None = None
    swap_route_inference: ImportRef | None = None
    teardown_post_condition: ImportRef | None = None
    deferred_refresh: ImportRef | None = None
    pool_reader: ImportRef | None = None
    capabilities: CapabilitiesSpec | None = None
    supported_chains: SupportedChainsSpec | None = None
    primitive: ImportRef | None = None
    swap_quote_connector: ImportRef | None = None
    accounting_treatment: ImportRef | None = None
    accounting_report: ImportRef | None = None
    gateway_settings: ImportRef | None = None
    gateway_connector: ImportRef | None = None
    gateway_connectors: tuple[ImportRef, ...] = field(default_factory=tuple)
    protocol_family: ImportRef | None = None
    swap_classification: ImportRef | None = None
    contract_monitoring: ImportRef | None = None
    contract_roles: ImportRef | None = None
    permission_infrastructure: ImportRef | None = None
    bridge_adapter: ImportRef | None = None
    compiler: ImportRef | None = None
    compiler_protocols: tuple[str, ...] | None = None
    compiler_default_keys: tuple[str, ...] = field(default_factory=tuple)
    flash_loan_provider_name: str | None = None
    flash_loan_provider: ImportRef | None = None
    flash_loan_builder: ImportRef | None = None
    flash_loan_synthetic_discovery: bool = False
    strategy_intents: tuple[str, ...] | None = None
    strategy_chains: tuple[str, ...] | None = None
    strategy_matrix_entries: tuple[StrategyMatrixEntry, ...] | None = None

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
        self._validate_address_tables()
        self._validate_solana_programs()
        self._validate_gateway_connectors()
        self._validate_receipt_parser_protocols()
        self._validate_gas_estimate_connector()
        self._validate_agent_read_connectors()
        self._validate_vault_tool_connectors()
        self._validate_runner_hook_connector()
        self._validate_protocol_metadata()
        self._validate_principal_token_market_reader()
        self._validate_swap_route_inference()
        self._validate_teardown_post_condition()
        self._validate_deferred_refresh()
        self._validate_pool_reader()
        self._validate_capabilities()
        self._validate_supported_chains()
        self._validate_primitive()
        self._validate_swap_quote_connector()
        self._validate_accounting_treatment()
        self._validate_accounting_report()
        self._validate_gateway_settings()
        self._validate_protocol_family()
        self._validate_swap_classification()
        self._validate_contract_monitoring()
        self._validate_contract_roles()
        self._validate_permission_infrastructure()
        self._validate_bridge_adapter()
        self._validate_compiler()
        self._validate_flash_loan()
        self._validate_strategy_support()

    def _validate_address_tables(self) -> None:
        """Validate strategy-side address-table selectors."""
        if self.address_tables is None:
            return
        if not isinstance(self.address_tables, tuple) or not self.address_tables:
            raise ValueError(
                "Connector.address_tables must be None or a non-empty tuple[AddressTableSpec, ...], "
                f"got {self.address_tables!r}"
            )
        bad_specs = [spec for spec in self.address_tables if not isinstance(spec, AddressTableSpec)]
        if bad_specs:
            raise ValueError(f"Connector.address_tables must contain only AddressTableSpec values, got {bad_specs!r}")
        protocols = [spec.protocol for spec in self.address_tables]
        if len(set(protocols)) != len(protocols):
            raise ValueError(f"Connector.address_tables contains duplicate protocols: {protocols!r}")

    def _validate_solana_programs(self) -> None:
        """Validate connector-owned Solana program clone specs."""
        if self.solana_programs is None:
            return
        if not isinstance(self.solana_programs, tuple) or not self.solana_programs:
            raise ValueError(
                "Connector.solana_programs must be None or a non-empty tuple[SolanaProgramSpec, ...], "
                f"got {self.solana_programs!r}"
            )
        bad_specs = [spec for spec in self.solana_programs if not isinstance(spec, SolanaProgramSpec)]
        if bad_specs:
            raise ValueError(f"Connector.solana_programs must contain only SolanaProgramSpec values, got {bad_specs!r}")
        protocols = [spec.protocol for spec in self.solana_programs]
        if len(set(protocols)) != len(protocols):
            raise ValueError(f"Connector.solana_programs contains duplicate protocols: {protocols!r}")
        program_ids = [spec.program_id for spec in self.solana_programs]
        if len(set(program_ids)) != len(program_ids):
            raise ValueError(f"Connector.solana_programs contains duplicate program IDs: {program_ids!r}")

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

    def _validate_runner_hook_connector(self) -> None:
        """Validate the strategy-runner hook provider import reference."""
        if self.runner_hook_connector is not None and not isinstance(self.runner_hook_connector, ImportRef):
            raise ValueError(
                f"Connector.runner_hook_connector must be None or an ImportRef, got {self.runner_hook_connector!r}"
            )

    def _validate_protocol_metadata(self) -> None:
        """Validate the strategy-side protocol-metadata provider import reference."""
        if self.protocol_metadata is not None and not isinstance(self.protocol_metadata, ImportRef):
            raise ValueError(
                f"Connector.protocol_metadata must be None or an ImportRef, got {self.protocol_metadata!r}"
            )

    def _validate_principal_token_market_reader(self) -> None:
        """Validate the strategy-side principal-token market reader import reference."""
        if self.principal_token_market_reader is not None and not isinstance(
            self.principal_token_market_reader, ImportRef
        ):
            raise ValueError(
                "Connector.principal_token_market_reader must be None or an ImportRef, "
                f"got {self.principal_token_market_reader!r}"
            )

    def _validate_swap_route_inference(self) -> None:
        """Validate the strategy-side swap-route inference import reference."""
        if self.swap_route_inference is not None and not isinstance(self.swap_route_inference, ImportRef):
            raise ValueError(
                f"Connector.swap_route_inference must be None or an ImportRef, got {self.swap_route_inference!r}"
            )

    def _validate_teardown_post_condition(self) -> None:
        """Validate the strategy-side teardown post-condition import reference."""
        if self.teardown_post_condition is not None and not isinstance(self.teardown_post_condition, ImportRef):
            raise ValueError(
                f"Connector.teardown_post_condition must be None or an ImportRef, got {self.teardown_post_condition!r}"
            )

    def _validate_deferred_refresh(self) -> None:
        """Validate the strategy-side deferred-refresh provider import reference."""
        if self.deferred_refresh is not None and not isinstance(self.deferred_refresh, ImportRef):
            raise ValueError(f"Connector.deferred_refresh must be None or an ImportRef, got {self.deferred_refresh!r}")

    def _validate_pool_reader(self) -> None:
        """Validate the strategy-side pool reader spec import reference."""
        if self.pool_reader is not None and not isinstance(self.pool_reader, ImportRef):
            raise ValueError(f"Connector.pool_reader must be None or an ImportRef, got {self.pool_reader!r}")

    def _validate_capabilities(self) -> None:
        """Validate the protocol-capabilities ownership spec."""
        if self.capabilities is not None and not isinstance(self.capabilities, CapabilitiesSpec):
            raise ValueError(f"Connector.capabilities must be None or a CapabilitiesSpec, got {self.capabilities!r}")

    def _validate_supported_chains(self) -> None:
        """Validate the chain-coverage ownership spec."""
        if self.supported_chains is not None and not isinstance(self.supported_chains, SupportedChainsSpec):
            raise ValueError(
                f"Connector.supported_chains must be None or a SupportedChainsSpec, got {self.supported_chains!r}"
            )

    def _validate_primitive(self) -> None:
        """Validate the position-primitive declaration import reference."""
        if self.primitive is not None and not isinstance(self.primitive, ImportRef):
            raise ValueError(f"Connector.primitive must be None or an ImportRef, got {self.primitive!r}")

    def _validate_swap_quote_connector(self) -> None:
        """Validate the strategy-side swap quote provider import reference."""
        if self.swap_quote_connector is not None and not isinstance(self.swap_quote_connector, ImportRef):
            raise ValueError(
                f"Connector.swap_quote_connector must be None or an ImportRef, got {self.swap_quote_connector!r}"
            )

    def _validate_accounting_treatment(self) -> None:
        """Validate the strategy-side accounting-treatment spec import reference."""
        if self.accounting_treatment is not None and not isinstance(self.accounting_treatment, ImportRef):
            raise ValueError(
                f"Connector.accounting_treatment must be None or an ImportRef, got {self.accounting_treatment!r}"
            )

    def _validate_accounting_report(self) -> None:
        """Validate the strategy-side accounting-report provider import reference."""
        if self.accounting_report is not None and not isinstance(self.accounting_report, ImportRef):
            raise ValueError(
                f"Connector.accounting_report must be None or an ImportRef, got {self.accounting_report!r}"
            )

    def _validate_gateway_settings(self) -> None:
        """Validate the gateway-side settings-fragment import reference."""
        if self.gateway_settings is not None and not isinstance(self.gateway_settings, ImportRef):
            raise ValueError(f"Connector.gateway_settings must be None or an ImportRef, got {self.gateway_settings!r}")

    def _validate_protocol_family(self) -> None:
        """Validate the protocol-family spec import reference."""
        if self.protocol_family is not None and not isinstance(self.protocol_family, ImportRef):
            raise ValueError(f"Connector.protocol_family must be None or an ImportRef, got {self.protocol_family!r}")

    def _validate_swap_classification(self) -> None:
        """Validate the swap-classification spec import reference."""
        if self.swap_classification is not None and not isinstance(self.swap_classification, ImportRef):
            raise ValueError(
                f"Connector.swap_classification must be None or an ImportRef, got {self.swap_classification!r}"
            )

    def _validate_contract_monitoring(self) -> None:
        """Validate the contract-monitoring spec import reference."""
        if self.contract_monitoring is not None and not isinstance(self.contract_monitoring, ImportRef):
            raise ValueError(
                f"Connector.contract_monitoring must be None or an ImportRef, got {self.contract_monitoring!r}"
            )

    def _validate_contract_roles(self) -> None:
        """Validate the contract-role spec import reference."""
        if self.contract_roles is not None and not isinstance(self.contract_roles, ImportRef):
            raise ValueError(f"Connector.contract_roles must be None or an ImportRef, got {self.contract_roles!r}")

    def _validate_permission_infrastructure(self) -> None:
        """Validate the infrastructure-permission builder import reference."""
        if self.permission_infrastructure is not None and not isinstance(self.permission_infrastructure, ImportRef):
            raise ValueError(
                "Connector.permission_infrastructure must be None or an ImportRef, "
                f"got {self.permission_infrastructure!r}"
            )

    def _validate_bridge_adapter(self) -> None:
        """Validate the bridge-adapter factory import reference."""
        if self.bridge_adapter is not None and not isinstance(self.bridge_adapter, ImportRef):
            raise ValueError(f"Connector.bridge_adapter must be None or an ImportRef, got {self.bridge_adapter!r}")

    def _validate_compiler(self) -> None:
        """Validate compiler import references and advertised protocol keys."""
        if self.compiler is not None and not isinstance(self.compiler, ImportRef):
            raise ValueError(f"Connector.compiler must be None or an ImportRef, got {self.compiler!r}")
        if self.compiler_protocols is not None:
            if self.compiler is None:
                raise ValueError("Connector.compiler_protocols may only be set when compiler is also set")
            self._validate_non_empty_string_tuple("compiler_protocols", self.compiler_protocols)
        if not isinstance(self.compiler_default_keys, tuple):
            raise ValueError(
                f"Connector.compiler_default_keys must be a tuple[str, ...], got {self.compiler_default_keys!r}"
            )
        if self.compiler_default_keys:
            if self.compiler is None:
                raise ValueError("Connector.compiler_default_keys may only be set when compiler is also set")
            self._validate_non_empty_string_tuple("compiler_default_keys", self.compiler_default_keys)

    @staticmethod
    def _validate_non_empty_string_tuple(field_name: str, value: tuple[str, ...]) -> None:
        """Validate a non-empty tuple of unique, non-blank strings."""
        if not isinstance(value, tuple) or not value:
            raise ValueError(f"Connector.{field_name} must be a non-empty tuple[str, ...], got {value!r}")
        bad_values = [item for item in value if not isinstance(item, str) or not item.strip()]
        if bad_values:
            raise ValueError(f"Connector.{field_name} must contain only non-empty strings, got {bad_values!r}")
        if len(set(value)) != len(value):
            raise ValueError(f"Connector.{field_name} contains duplicates: {value!r}")

    def _validate_flash_loan(self) -> None:
        """Validate flash-loan provider import references and metadata."""
        if self.flash_loan_provider is not None and not isinstance(self.flash_loan_provider, ImportRef):
            raise ValueError(
                f"Connector.flash_loan_provider must be None or an ImportRef, got {self.flash_loan_provider!r}"
            )
        if self.flash_loan_builder is not None and not isinstance(self.flash_loan_builder, ImportRef):
            raise ValueError(
                f"Connector.flash_loan_builder must be None or an ImportRef, got {self.flash_loan_builder!r}"
            )
        if not isinstance(self.flash_loan_synthetic_discovery, bool):
            raise ValueError(
                f"Connector.flash_loan_synthetic_discovery must be a bool, got {self.flash_loan_synthetic_discovery!r}"
            )
        has_flash_loan = (
            self.flash_loan_provider_name is not None
            or self.flash_loan_provider is not None
            or self.flash_loan_builder is not None
            or self.flash_loan_synthetic_discovery
        )
        if has_flash_loan:
            if not isinstance(self.flash_loan_provider_name, str) or not self.flash_loan_provider_name.strip():
                raise ValueError(
                    "Connector.flash_loan_provider_name must be a non-empty string when flash-loan refs are set, "
                    f"got {self.flash_loan_provider_name!r}"
                )
            if self.flash_loan_provider is None:
                raise ValueError("Connector.flash_loan_provider is required when flash-loan metadata is set")
            if self.flash_loan_builder is None:
                raise ValueError("Connector.flash_loan_builder is required when flash-loan metadata is set")

    def _validate_strategy_support(self) -> None:
        """Validate optional strategy-side registration metadata."""
        if self.strategy_intents is None:
            if self.strategy_chains is not None:
                raise ValueError("Connector.strategy_chains may only be set when strategy_intents is set")
            if self.strategy_matrix_entries is not None:
                raise ValueError("Connector.strategy_matrix_entries may only be set when strategy_intents is set")
            return

        if not isinstance(self.strategy_intents, tuple) or not self.strategy_intents:
            raise ValueError(
                f"Connector.strategy_intents must be None or a non-empty tuple[str, ...], got {self.strategy_intents!r}"
            )
        bad_intents = [intent for intent in self.strategy_intents if not isinstance(intent, str) or not intent.strip()]
        if bad_intents:
            raise ValueError(f"Connector.strategy_intents must contain only non-empty strings, got {bad_intents!r}")
        if len(set(self.strategy_intents)) != len(self.strategy_intents):
            raise ValueError(f"Connector.strategy_intents contains duplicates: {self.strategy_intents!r}")

        self._validate_strategy_chains()
        self._validate_strategy_matrix_entries()

    def _validate_strategy_chains(self) -> None:
        """Validate strategy-side chain identifiers without importing chain registries."""
        if self.strategy_chains is None:
            return
        if not isinstance(self.strategy_chains, tuple) or not self.strategy_chains:
            raise ValueError(
                "Connector.strategy_chains must be None or a non-empty tuple[str, ...], "
                f"got {self.strategy_chains!r}. Use strategy_chains=None for off-chain venues."
            )
        bad_chains = [chain for chain in self.strategy_chains if not isinstance(chain, str) or not chain.strip()]
        if bad_chains:
            raise ValueError(f"Connector.strategy_chains must contain only non-empty strings, got {bad_chains!r}")
        if len(set(self.strategy_chains)) != len(self.strategy_chains):
            raise ValueError(f"Connector.strategy_chains contains duplicates: {self.strategy_chains!r}")

    def _validate_strategy_matrix_entries(self) -> None:
        """Validate descriptor-owned support-matrix rows."""
        if self.strategy_matrix_entries is None:
            return
        if not isinstance(self.strategy_matrix_entries, tuple):
            raise ValueError(
                "Connector.strategy_matrix_entries must be None or a tuple[StrategyMatrixEntry, ...], "
                f"got {self.strategy_matrix_entries!r}"
            )
        bad_entries = [entry for entry in self.strategy_matrix_entries if not isinstance(entry, StrategyMatrixEntry)]
        if bad_entries:
            raise ValueError(
                f"Connector.strategy_matrix_entries must contain only StrategyMatrixEntry values, got {bad_entries!r}"
            )
        for entry in self.strategy_matrix_entries:
            self._validate_strategy_matrix_entry_fields(entry)
        keys = [(entry.matrix_name, entry.category) for entry in self.strategy_matrix_entries]
        if len(set(keys)) != len(keys):
            raise ValueError(f"Connector.strategy_matrix_entries has duplicate (matrix_name, category) keys: {keys!r}")

    @staticmethod
    def _validate_strategy_matrix_entry_fields(entry: StrategyMatrixEntry) -> None:
        """Validate one strategy support-matrix row's fields."""
        if not isinstance(entry.matrix_name, str) or not entry.matrix_name.strip():
            raise ValueError(f"StrategyMatrixEntry.matrix_name must be a non-empty string, got {entry.matrix_name!r}")
        if not isinstance(entry.category, str) or not entry.category.strip():
            raise ValueError(f"StrategyMatrixEntry.category must be a non-empty string, got {entry.category!r}")
        if not isinstance(entry.chains, frozenset) or not entry.chains:
            raise ValueError(f"StrategyMatrixEntry.chains must be a non-empty frozenset[str], got {entry.chains!r}")
        bad_chains = [chain for chain in entry.chains if not isinstance(chain, str) or not chain.strip()]
        if bad_chains:
            raise ValueError(f"StrategyMatrixEntry.chains must contain only non-empty strings, got {bad_chains!r}")

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
    def compiler_keys(self) -> frozenset[str]:
        """Protocol keys resolved by this connector's compiler.

        Defaults to the connector identity keys. Connectors whose compiler
        protocol vocabulary differs from discovery aliases can set
        ``compiler_protocols`` explicitly.
        """
        if self.compiler is None:
            return frozenset()
        if self.compiler_protocols is None:
            return self.protocol_keys
        return frozenset(self.compiler_protocols)

    @property
    def has_strategy_support(self) -> bool:
        """Whether this manifest owns strategy-side registry metadata."""
        return self.strategy_intents is not None

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

    def with_address_tables(self) -> tuple[Connector, ...]:
        """Return connectors that publish strategy-side address-table specs."""
        return tuple(d for d in self.all() if d.address_tables is not None)

    def with_solana_programs(self) -> tuple[Connector, ...]:
        """Return connectors that publish local Solana program clone specs."""
        return tuple(d for d in self.all() if d.solana_programs is not None)

    def with_gas_estimate(self) -> tuple[Connector, ...]:
        """Return connectors that publish a gas-estimate connector."""
        return tuple(d for d in self.all() if d.gas_estimate_connector is not None)

    def with_agent_read(self) -> tuple[Connector, ...]:
        """Return connectors that publish agent-read connectors."""
        return tuple(d for d in self.all() if d.agent_read_connector_refs)

    def with_vault_tool(self) -> tuple[Connector, ...]:
        """Return connectors that publish vault-tool connectors."""
        return tuple(d for d in self.all() if d.vault_tool_connector_refs)

    def with_runner_hooks(self) -> tuple[Connector, ...]:
        """Return connectors that publish strategy-runner hook connectors."""
        return tuple(d for d in self.all() if d.runner_hook_connector is not None)

    def with_protocol_metadata(self) -> tuple[Connector, ...]:
        """Return connectors that publish protocol metadata providers."""
        return tuple(d for d in self.all() if d.protocol_metadata is not None)

    def with_principal_token_market_reader(self) -> tuple[Connector, ...]:
        """Return connectors that publish principal-token market readers."""
        return tuple(d for d in self.all() if d.principal_token_market_reader is not None)

    def with_swap_route_inference(self) -> tuple[Connector, ...]:
        """Return connectors that publish swap-route inference providers."""
        return tuple(d for d in self.all() if d.swap_route_inference is not None)

    def with_teardown_post_condition(self) -> tuple[Connector, ...]:
        """Return connectors that publish teardown post-condition hooks."""
        return tuple(d for d in self.all() if d.teardown_post_condition is not None)

    def with_deferred_refresh(self) -> tuple[Connector, ...]:
        """Return connectors that publish deferred transaction refresh providers."""
        return tuple(d for d in self.all() if d.deferred_refresh is not None)

    def with_pool_reader(self) -> tuple[Connector, ...]:
        """Return connectors that publish pool reader specs."""
        return tuple(d for d in self.all() if d.pool_reader is not None)

    def with_capabilities(self) -> tuple[Connector, ...]:
        """Return connectors that publish protocol-capability ownership specs."""
        return tuple(d for d in self.all() if d.capabilities is not None)

    def with_supported_chains(self) -> tuple[Connector, ...]:
        """Return connectors that publish chain-coverage ownership specs."""
        return tuple(d for d in self.all() if d.supported_chains is not None)

    def with_primitive(self) -> tuple[Connector, ...]:
        """Return connectors that publish position-primitive declarations."""
        return tuple(d for d in self.all() if d.primitive is not None)

    def with_swap_quote(self) -> tuple[Connector, ...]:
        """Return connectors that publish swap quote providers."""
        return tuple(d for d in self.all() if d.swap_quote_connector is not None)

    def with_accounting_treatment(self) -> tuple[Connector, ...]:
        """Return connectors that publish accounting-treatment specs."""
        return tuple(d for d in self.all() if d.accounting_treatment is not None)

    def with_accounting_report(self) -> tuple[Connector, ...]:
        """Return connectors that publish accounting-report providers."""
        return tuple(d for d in self.all() if d.accounting_report is not None)

    def with_gateway_settings(self) -> tuple[Connector, ...]:
        """Return connectors that publish gateway settings fragments."""
        return tuple(d for d in self.all() if d.gateway_settings is not None)

    def with_protocol_family(self) -> tuple[Connector, ...]:
        """Return connectors that publish protocol-family specs."""
        return tuple(d for d in self.all() if d.protocol_family is not None)

    def with_swap_classification(self) -> tuple[Connector, ...]:
        """Return connectors that publish swap-classification specs."""
        return tuple(d for d in self.all() if d.swap_classification is not None)

    def with_contract_monitoring(self) -> tuple[Connector, ...]:
        """Return connectors that publish contract-monitoring specs."""
        return tuple(d for d in self.all() if d.contract_monitoring is not None)

    def with_contract_roles(self) -> tuple[Connector, ...]:
        """Return connectors that publish contract-role specs."""
        return tuple(d for d in self.all() if d.contract_roles is not None)

    def with_permission_infrastructure(self) -> tuple[Connector, ...]:
        """Return connectors that publish infrastructure-permission builders."""
        return tuple(d for d in self.all() if d.permission_infrastructure is not None)

    def with_bridge_adapter(self) -> tuple[Connector, ...]:
        """Return connectors that publish bridge-adapter factories."""
        return tuple(d for d in self.all() if d.bridge_adapter is not None)

    def with_compiler(self) -> tuple[Connector, ...]:
        """Return connectors that publish intent compilers."""
        return tuple(d for d in self.all() if d.compiler is not None)

    def with_flash_loan(self) -> tuple[Connector, ...]:
        """Return connectors that publish flash-loan providers."""
        return tuple(d for d in self.all() if d.flash_loan_provider is not None)

    def with_strategy_support(self) -> tuple[Connector, ...]:
        """Return connectors that publish strategy-side registration metadata."""
        return tuple(d for d in self.all() if d.has_strategy_support)

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
        seen_contract_role_orders: dict[int, str] = {}
        seen_swap_classification_orders: dict[int, str] = {}
        seen_bridge_adapter_orders: dict[int, str] = {}
        seen_flash_loan_provider_orders: dict[int, str] = {}
        seen_gateway_settings_orders: dict[int, str] = {}
        seen_compiler_keys: dict[str, str] = {}
        seen_compiler_default_keys: dict[str, str] = {}
        seen_capability_keys: dict[str, str] = {}
        seen_supported_chain_keys: dict[str, str] = {}

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
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Gateway connector",
                refs=connector.gateway_connector_refs,
                seen_orders=seen_gateway_orders,
            )
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Contract-role",
                refs=() if connector.contract_roles is None else (connector.contract_roles,),
                seen_orders=seen_contract_role_orders,
            )
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Swap-classification",
                refs=() if connector.swap_classification is None else (connector.swap_classification,),
                seen_orders=seen_swap_classification_orders,
            )
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Bridge adapter",
                refs=() if connector.bridge_adapter is None else (connector.bridge_adapter,),
                seen_orders=seen_bridge_adapter_orders,
            )
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Flash-loan provider",
                refs=() if connector.flash_loan_provider is None else (connector.flash_loan_provider,),
                seen_orders=seen_flash_loan_provider_orders,
            )
            self._validate_unique_ref_order(
                connector_name=connector.name,
                capability="Gateway settings",
                refs=() if connector.gateway_settings is None else (connector.gateway_settings,),
                seen_orders=seen_gateway_settings_orders,
            )
            self._validate_unique_compiler_keys(
                connector=connector,
                seen_compiler_keys=seen_compiler_keys,
                seen_compiler_default_keys=seen_compiler_default_keys,
            )
            self._validate_unique_ownership_keys(
                connector_name=connector.name,
                capability="Capabilities",
                keys=() if connector.capabilities is None else connector.capabilities.keys,
                seen_keys=seen_capability_keys,
            )
            self._validate_unique_ownership_keys(
                connector_name=connector.name,
                capability="Supported-chains",
                keys=() if connector.supported_chains is None else connector.supported_chains.keys,
                seen_keys=seen_supported_chain_keys,
            )
            connectors.append(connector)

        return tuple(sorted(connectors, key=lambda d: d.name))

    @staticmethod
    def _validate_unique_ownership_keys(
        *,
        connector_name: str,
        capability: str,
        keys: tuple[str, ...],
        seen_keys: dict[str, str],
    ) -> None:
        """Reject one metadata-ownership key claimed by two connector manifests."""
        for key in keys:
            owner = seen_keys.get(key)
            if owner is not None:
                raise ConnectorDiscoveryError(
                    f"{capability} key {key!r} is claimed by both {owner!r} and {connector_name!r}"
                )
            seen_keys[key] = connector_name

    @staticmethod
    def _validate_unique_ref_order(
        *,
        connector_name: str,
        capability: str,
        refs: tuple[ImportRef, ...],
        seen_orders: dict[int, str],
    ) -> None:
        """Reject duplicate explicit order keys for one order-bearing capability."""
        for import_ref in refs:
            if import_ref.order is None:
                continue
            owner = seen_orders.get(import_ref.order)
            if owner is not None:
                raise ConnectorDiscoveryError(
                    f"{capability} order {import_ref.order} is claimed by both {owner!r} and {connector_name!r}"
                )
            seen_orders[import_ref.order] = connector_name

    @staticmethod
    def _validate_unique_compiler_keys(
        *,
        connector: Connector,
        seen_compiler_keys: dict[str, str],
        seen_compiler_default_keys: dict[str, str],
    ) -> None:
        """Reject duplicate compiler protocol and dispatch-default claims."""
        for key in connector.compiler_keys:
            normalized_key = key.strip().lower().replace("-", "_")
            owner = seen_compiler_keys.get(normalized_key)
            if owner is not None:
                raise ConnectorDiscoveryError(
                    f"Compiler protocol {normalized_key!r} is claimed by both {owner!r} and {connector.name!r}"
                )
            seen_compiler_keys[normalized_key] = connector.name
        for key in connector.compiler_default_keys:
            normalized_key = key.strip().upper()
            owner = seen_compiler_default_keys.get(normalized_key)
            if owner is not None:
                raise ConnectorDiscoveryError(
                    f"Compiler default key {normalized_key!r} is claimed by both {owner!r} and {connector.name!r}"
                )
            seen_compiler_default_keys[normalized_key] = connector.name

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
