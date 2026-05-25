"""ChainDescriptor — single source of truth for per-chain configuration.

A ``ChainDescriptor`` consolidates everything the SDK needs to know about a
single chain:

* Identity: ``enum`` (the ``Chain`` enum member), ``name`` (canonical lowercase
  string), ``aliases`` (e.g. ``"bnb"`` for ``Chain.BSC``).
* Wire format: ``chain_id`` (EIP-155). The numeric value is the on-the-wire
  identifier owned by the ``metrics-database`` repo — restructuring how we
  source it in the SDK is fine, **renumbering it is not**.
* Family: ``family`` (EVM vs SOLANA — routes signing / address format / tx model).
* Native token: ``NativeToken`` (symbol, name, decimals, wrapped address).
* Gas profile: ``GasProfile`` (buffer multiplier, price/cost caps, simulation buffer).
* Timeouts: ``Timeouts`` (tx confirmation, gRPC Execute call).

Per-chain descriptor files live as siblings (``ethereum.py``, ``arbitrum.py``,
``base.py``, ...). Each registers itself via ``@register_chain`` into the
singleton ``ChainRegistry`` at import time.

VIB-4801 (parent epic VIB-4800).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from almanak.core.enums import Chain, ChainFamily


@dataclass(frozen=True)
class NativeToken:
    """Native-gas token metadata for a chain.

    Attributes:
        symbol: e.g. ``"ETH"``, ``"AVAX"``, ``"BNB"``.
        name: Human-readable name (e.g. ``"Ethereum"``, ``"BNB"``).
        decimals: Decimal places (18 for every EVM chain, 9 for SOL).
        wrapped_address: Address of the wrapped ERC-20 (or SPL mint for Solana).
            ``None`` is reserved for chains with no canonical wrapper — every
            chain currently registered has one.
    """

    symbol: str
    name: str
    decimals: int
    wrapped_address: str | None = None


@dataclass(frozen=True)
class GasProfile:
    """Per-chain gas knobs.

    Every field is :data:`Optional` — ``None`` means "this chain has no
    entry in the corresponding legacy dict; let the consumer's
    ``.get(chain, DEFAULT)`` fall back". The legacy dicts had asymmetric
    coverage (e.g. ``CHAIN_GAS_COST_CAPS_NATIVE`` only covered 12 of 16
    EVM chains), and we preserve that asymmetry byte-for-byte to avoid
    behavior changes at the lookup boundary.

    Attributes:
        buffer: Multiplier applied to raw gas estimates from simulation /
            ``eth_estimateGas`` (mirrors ``CHAIN_GAS_BUFFERS``).
        simulation_buffer: Decimal fraction added on top of post-simulation
            gas (mirrors ``CHAIN_SIMULATION_BUFFERS``; 0.1 == 10%).
        price_cap_gwei: Recommended maximum gas price in gwei
            (mirrors ``CHAIN_GAS_PRICE_CAPS_GWEI``).
        cost_cap_native: Recommended maximum gas cost in native units
            (mirrors ``CHAIN_GAS_COST_CAPS_NATIVE``).
    """

    buffer: float | None = None
    simulation_buffer: float | None = None
    price_cap_gwei: int | None = None
    cost_cap_native: float | None = None


@dataclass(frozen=True)
class Timeouts:
    """Per-chain timeouts.

    Attributes:
        tx_confirmation: Seconds to wait for a tx to land
            (mirrors ``CHAIN_TX_TIMEOUTS``). ``None`` falls back to the
            framework default.
        grpc_execute: Seconds for the gateway gRPC ``Execute`` call
            (mirrors ``CHAIN_GRPC_EXECUTE_TIMEOUTS``). ``None`` falls back to
            the framework default.
    """

    tx_confirmation: int | None = None
    grpc_execute: int | None = None


@dataclass(frozen=True)
class ChainDescriptor:
    """Single source of truth for per-chain configuration.

    Construction is always through a ``@register_chain``-decorated module
    under ``almanak/core/chains/``. Consumers read via ``ChainRegistry``;
    descriptors are immutable.

    Attributes:
        enum: The :class:`Chain` enum member.
        name: Canonical lowercase name (e.g. ``"ethereum"``).
            Always equal to ``enum.name.lower()`` — never diverge.
        chain_id: EIP-155 chain ID. ``0`` is reserved for non-EVM chains
            (Solana).
        family: Execution family (EVM vs SOLANA).
        native: ``NativeToken`` — symbol, decimals, wrapped address.
        gas: ``GasProfile`` — buffer, caps, simulation buffer.
        timeouts: ``Timeouts`` — tx confirmation + gRPC Execute.
        aliases: Extra alternative names that resolve to this chain
            (e.g. ``("bnb", "binance")`` for BSC). The canonical ``name``
            is always implicit and need not be repeated here.
    """

    enum: Chain
    name: str
    chain_id: int
    family: ChainFamily
    native: NativeToken
    gas: GasProfile
    timeouts: Timeouts = field(default_factory=Timeouts)
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Strong invariant: ``name`` always equals the lowercase enum name.
        # If they drift, downstream lookups break in subtle ways.
        if self.name != self.enum.name.lower():
            raise ValueError(
                f"ChainDescriptor.name {self.name!r} must equal enum name "
                f"{self.enum.name.lower()!r} (enum: {self.enum.name})"
            )
