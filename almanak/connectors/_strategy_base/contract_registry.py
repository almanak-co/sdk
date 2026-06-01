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

from almanak.connectors._strategy_base.address_registry import AddressRegistry
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


@dataclass(frozen=True)
class _ProtocolDef:
    """One (protocol, contract-kind) → receipt-parser binding.

    ``protocol`` is the :class:`AddressRegistry` identifier whose per-chain
    address table supplies the on-chain address for ``contract_key``; the
    registry brokers the connector's ``addresses.py`` so this table never
    imports a connector module directly.
    """

    protocol: str
    contract_key: str
    parser_module: str
    parser_class: str
    actions: tuple[str, ...]


_PROTOCOL_DEFS: tuple[_ProtocolDef, ...] = (
    # DEX swap routers
    _ProtocolDef(
        "uniswap_v3",
        "swap_router",
        "almanak.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "agni_finance",
        "swap_router",
        "almanak.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "pancakeswap_v3",
        "swap_router",
        "almanak.connectors.pancakeswap_v3.receipt_parser",
        "PancakeSwapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "sushiswap_v3",
        "swap_router",
        "almanak.connectors.sushiswap_v3.receipt_parser",
        "SushiSwapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "aerodrome",
        "router",
        "almanak.connectors.aerodrome.receipt_parser",
        "AerodromeReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "traderjoe_v2",
        "router",
        "almanak.connectors.traderjoe_v2.receipt_parser",
        "TraderJoeV2ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "pendle",
        "router",
        "almanak.connectors.pendle.receipt_parser",
        "PendleReceiptParser",
        ("SWAP", "LP_OPEN", "LP_CLOSE"),
    ),
    # Uniswap V4 — singleton PoolManager handles swaps, PositionManager handles LP
    _ProtocolDef(
        "uniswap_v4",
        "pool_manager",
        "almanak.connectors.uniswap_v4.receipt_parser",
        "UniswapV4ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        "uniswap_v4",
        "position_manager",
        "almanak.connectors.uniswap_v4.receipt_parser",
        "UniswapV4ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    # LP managers
    _ProtocolDef(
        "uniswap_v3",
        "position_manager",
        "almanak.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        "agni_finance",
        "position_manager",
        "almanak.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        "pancakeswap_v3",
        "position_manager",
        "almanak.connectors.pancakeswap_v3.receipt_parser",
        "PancakeSwapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        "sushiswap_v3",
        "position_manager",
        "almanak.connectors.sushiswap_v3.receipt_parser",
        "SushiSwapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    # Lending
    _ProtocolDef(
        "aave_v3",
        "pool",
        "almanak.connectors.aave_v3.receipt_parser",
        "AaveV3ReceiptParser",
        ("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
    _ProtocolDef(
        "morpho_blue",
        "morpho",
        "almanak.connectors.morpho_blue.receipt_parser",
        "MorphoBlueReceiptParser",
        ("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
    # Perpetuals
    _ProtocolDef(
        "gmx_v2",
        "exchange_router",
        "almanak.connectors.gmx_v2.receipt_parser",
        "GMXv2ReceiptParser",
        ("PERP_OPEN", "PERP_CLOSE"),
    ),
)


def get_default_registry() -> ContractRegistry:
    """Create a ContractRegistry populated from connector-owned address tables.

    Each protocol's per-chain address table is resolved through the
    strategy-side :class:`AddressRegistry` (W1 / VIB-4853), which brokers
    the connector's ``addresses.py``. The previous central registry at
    ``almanak.core.contracts`` has been deleted.
    """
    registry = ContractRegistry()
    for definition in _PROTOCOL_DEFS:
        for chain in AddressRegistry.address_supported_chains(definition.protocol):
            contracts = AddressRegistry.addresses_for(definition.protocol, chain)
            address = contracts.get(definition.contract_key)
            if not address:
                continue
            registry.register(
                chain,
                address,
                ContractInfo(
                    protocol=definition.protocol,
                    contract_type=definition.contract_key,
                    parser_module=definition.parser_module,
                    parser_class_name=definition.parser_class,
                    supported_actions=list(definition.actions),
                ),
            )

        # Pendle has dynamic market addresses (`market_*`) where LP actions happen.
        if definition.protocol == "pendle":
            for chain in AddressRegistry.address_supported_chains(definition.protocol):
                contracts = AddressRegistry.addresses_for(definition.protocol, chain)
                for key, address in contracts.items():
                    if not key.startswith("market_"):
                        continue
                    registry.register(
                        chain,
                        address,
                        ContractInfo(
                            protocol="pendle",
                            contract_type=key,
                            parser_module="almanak.connectors.pendle.receipt_parser",
                            parser_class_name="PendleReceiptParser",
                            supported_actions=["SWAP", "LP_OPEN", "LP_CLOSE"],
                        ),
                    )

    return registry
