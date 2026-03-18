"""Contract address registry for protocol contract addresses.

Maps known contract addresses per chain to protocol metadata and action
capabilities. Used by copy-trading signal decoding for fail-closed behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from almanak.core.constants import resolve_chain_name
from almanak.core.contracts import (
    AAVE_V3,
    AERODROME,
    AGNI_FINANCE,
    GMX_V2,
    MORPHO_BLUE,
    PANCAKESWAP_V3,
    PENDLE,
    SUSHISWAP_V3,
    TRADERJOE_V2,
    UNISWAP_V3,
)


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
    addresses_dict: dict[str, dict[str, str]]
    contract_key: str
    protocol: str
    parser_module: str
    parser_class: str
    actions: tuple[str, ...]


_PROTOCOL_DEFS: tuple[_ProtocolDef, ...] = (
    # DEX swap routers
    _ProtocolDef(
        UNISWAP_V3,
        "swap_router",
        "uniswap_v3",
        "almanak.framework.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        AGNI_FINANCE,
        "swap_router",
        "agni_finance",
        "almanak.framework.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        PANCAKESWAP_V3,
        "swap_router",
        "pancakeswap_v3",
        "almanak.framework.connectors.pancakeswap_v3.receipt_parser",
        "PancakeSwapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        SUSHISWAP_V3,
        "swap_router",
        "sushiswap_v3",
        "almanak.framework.connectors.sushiswap_v3.receipt_parser",
        "SushiSwapV3ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        AERODROME,
        "router",
        "aerodrome",
        "almanak.framework.connectors.aerodrome.receipt_parser",
        "AerodromeReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        TRADERJOE_V2,
        "router",
        "traderjoe_v2",
        "almanak.framework.connectors.traderjoe_v2.receipt_parser",
        "TraderJoeV2ReceiptParser",
        ("SWAP",),
    ),
    _ProtocolDef(
        PENDLE,
        "router",
        "pendle",
        "almanak.framework.connectors.pendle.receipt_parser",
        "PendleReceiptParser",
        ("SWAP", "LP_OPEN", "LP_CLOSE"),
    ),
    # LP managers
    _ProtocolDef(
        UNISWAP_V3,
        "position_manager",
        "uniswap_v3",
        "almanak.framework.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        AGNI_FINANCE,
        "position_manager",
        "agni_finance",
        "almanak.framework.connectors.uniswap_v3.receipt_parser",
        "UniswapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        PANCAKESWAP_V3,
        "position_manager",
        "pancakeswap_v3",
        "almanak.framework.connectors.pancakeswap_v3.receipt_parser",
        "PancakeSwapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    _ProtocolDef(
        SUSHISWAP_V3,
        "position_manager",
        "sushiswap_v3",
        "almanak.framework.connectors.sushiswap_v3.receipt_parser",
        "SushiSwapV3ReceiptParser",
        ("LP_OPEN", "LP_CLOSE"),
    ),
    # Lending
    _ProtocolDef(
        AAVE_V3,
        "pool",
        "aave_v3",
        "almanak.framework.connectors.aave_v3.receipt_parser",
        "AaveV3ReceiptParser",
        ("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
    _ProtocolDef(
        MORPHO_BLUE,
        "morpho",
        "morpho_blue",
        "almanak.framework.connectors.morpho_blue.receipt_parser",
        "MorphoBlueReceiptParser",
        ("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
    # Perpetuals
    _ProtocolDef(
        GMX_V2,
        "exchange_router",
        "gmx_v2",
        "almanak.framework.connectors.gmx_v2.receipt_parser",
        "GMXv2ReceiptParser",
        ("PERP_OPEN", "PERP_CLOSE"),
    ),
)


def get_default_registry() -> ContractRegistry:
    """Create a ContractRegistry populated from `almanak.core.contracts`."""
    registry = ContractRegistry()
    for definition in _PROTOCOL_DEFS:
        for chain, contracts in definition.addresses_dict.items():
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
            for chain, contracts in definition.addresses_dict.items():
                for key, address in contracts.items():
                    if not key.startswith("market_"):
                        continue
                    registry.register(
                        chain,
                        address,
                        ContractInfo(
                            protocol="pendle",
                            contract_type=key,
                            parser_module="almanak.framework.connectors.pendle.receipt_parser",
                            parser_class_name="PendleReceiptParser",
                            supported_actions=["SWAP", "LP_OPEN", "LP_CLOSE"],
                        ),
                    )

    return registry
