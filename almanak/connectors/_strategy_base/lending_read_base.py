"""Strategy-side shared infrastructure for connector lending-position reads.

The framework's :class:`~almanak.framework.valuation.lending_position_reader.LendingPositionReader`
needs to read a wallet's current on-chain supply/debt for a single reserve so
valuation, position discovery, and ``amount="all"`` resolution can reprice
lending positions. *How* that read is performed — which on-chain contract holds
the per-user reserve data, the function selector, the calldata layout, and the
return decoding — is **connector knowledge**, not framework knowledge.

This module owns the strategy-side half every lending connector that exposes a
"read a single reserve position" capability shares:

* :class:`LendingPositionOnChain` — the canonical decoded result the framework
  consumes (re-exported by the framework reader for backward compatibility).
* :class:`LendingReadSpec` — the per-capability descriptor a connector publishes:
  the contract-kind it reads from (resolved through ``AddressRegistry``), the
  selector, the calldata encoder, and the return decoder.
* :data:`AAVE_FORK_RESERVE_READ` — the concrete spec for the Aave V3 fork
  family (Aave V3, Spark). Both forks expose the identical
  ``getUserReserveData(address asset, address user)`` ABI against their own
  ``pool_data_provider`` contract, so they share one spec; only the per-chain
  data-provider address (owned by each connector's ``addresses.py``) differs.

Gateway-boundary note: this module performs **no** network egress. It only
*describes* a read (selector + calldata + decoder) as pure data + pure
functions; the gateway-routed ``eth_call`` that executes the read stays in the
framework reader, which owns the gateway client.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

__all__ = [
    "AAVE_FORK_RESERVE_READ",
    "LendingPositionOnChain",
    "LendingReadSpec",
    "decode_uint_hex",
    "pad_address",
    "parse_user_reserve_data_hex",
]


@dataclass
class LendingPositionOnChain:
    """On-chain state of a lending position for a single reserve asset.

    Canonical home for the result the framework lending reader returns. Decoded
    from a connector's per-user reserve read (e.g. Aave-fork
    ``PoolDataProvider.getUserReserveData(asset, user)``). All amounts are in the
    reserve asset's native wei.
    """

    asset_address: str
    current_atoken_balance: int  # Supply + accrued interest (wei)
    current_stable_debt: int  # Stable rate debt (wei)
    current_variable_debt: int  # Variable rate debt (wei)
    liquidity_rate: int  # Supply APY in ray (1e27)
    usage_as_collateral_enabled: bool

    @property
    def is_active(self) -> bool:
        """Position has any supply or debt."""
        return self.current_atoken_balance > 0 or self.total_debt > 0

    @property
    def total_debt(self) -> int:
        """Total debt = stable + variable."""
        return self.current_stable_debt + self.current_variable_debt


@dataclass(frozen=True)
class LendingReadSpec:
    """Connector-published descriptor for a single-reserve lending read.

    Carries the protocol-specific knowledge the framework reader must NOT
    hardcode:

    Attributes:
        contract_kinds: Ordered contract-kind names (the connector's private
            ``addresses.py`` vocabulary) to resolve the read target from, tried
            in order via ``AddressRegistry.resolve_contract_address``. For the
            Aave fork family this is ``("pool_data_provider",)``.
        build_calldata: ``(asset_address, wallet_address) -> hex calldata`` for
            the read (selector + ABI-encoded args), without a ``0x`` prefix
            requirement on the result (the framework reader passes it verbatim).
        parse_result: ``(result_hex, asset_address) -> LendingPositionOnChain |
            None`` decoder for the read's return data.
    """

    contract_kinds: tuple[str, ...]
    build_calldata: Callable[[str, str], str]
    parse_result: Callable[[str, str], LendingPositionOnChain | None]


# ---------------------------------------------------------------------------
# Shared ABI helpers (Aave V2 / V3 fork family)
# ---------------------------------------------------------------------------


def pad_address(address: str) -> str:
    """Left-pad an address to 32 bytes (64 hex chars), no ``0x`` prefix."""
    addr = address.lower().replace("0x", "")
    return addr.zfill(64)


def decode_uint_hex(hex_data: str, word_index: int) -> int:
    """Decode a uint256 from ABI-encoded hex at the given 32-byte word index."""
    # Strip any 0x/0X prefix first so the word offset is correct for word_index > 0
    # (``int(..., 16)`` tolerates the prefix at index 0, but the slice would not).
    data = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data
    start = word_index * 64
    return int(data[start : start + 64], 16)


# Function selector for getUserReserveData(address asset, address user)
_GET_USER_RESERVE_DATA_SELECTOR = "0x28dd2d01"


def _build_get_user_reserve_data_calldata(asset_address: str, wallet_address: str) -> str:
    """Build calldata for ``getUserReserveData(address asset, address user)``."""
    return _GET_USER_RESERVE_DATA_SELECTOR + pad_address(asset_address) + pad_address(wallet_address)


def parse_user_reserve_data_hex(
    hex_data: str,
    asset_address: str,
) -> LendingPositionOnChain | None:
    """Parse hex response from Aave-fork ``getUserReserveData``.

    Expected ABI layout (9 words * 32 bytes = 576 hex chars):
    [0] currentATokenBalance (uint256)
    [1] currentStableDebt (uint256)
    [2] currentVariableDebt (uint256)
    [3] principalStableDebt (uint256) -- not used
    [4] scaledVariableDebt (uint256)  -- not used
    [5] stableBorrowRate (uint256)    -- not used
    [6] liquidityRate (uint256)
    [7] stableRateLastUpdated (uint40 padded) -- not used
    [8] usageAsCollateralEnabled (bool padded)
    """
    # Strip a leading 0x/0X prefix case-insensitively. A bare ``.replace("0x", "")``
    # would miss the upper-case form and could mangle a mid-string match.
    data = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data

    # 9 words * 64 hex chars = 576 minimum
    if len(data) < 576:
        logger.warning("getUserReserveData response too short: %d chars", len(data))
        return None

    try:
        atoken_balance = decode_uint_hex(data, 0)
        stable_debt = decode_uint_hex(data, 1)
        variable_debt = decode_uint_hex(data, 2)
        liquidity_rate = decode_uint_hex(data, 6)
        collateral_enabled = decode_uint_hex(data, 8) != 0

        return LendingPositionOnChain(
            asset_address=asset_address,
            current_atoken_balance=atoken_balance,
            current_stable_debt=stable_debt,
            current_variable_debt=variable_debt,
            liquidity_rate=liquidity_rate,
            usage_as_collateral_enabled=collateral_enabled,
        )
    except Exception:
        logger.debug("Failed to parse user reserve data hex", exc_info=True)
        return None


#: Read capability shared by every Aave V3 fork (Aave V3, Spark).
#: The forks expose the identical ``getUserReserveData`` ABI against their
#: own ``pool_data_provider`` contract; only the per-chain address (owned by
#: each connector's ``addresses.py``) differs.
AAVE_FORK_RESERVE_READ = LendingReadSpec(
    contract_kinds=("pool_data_provider",),
    build_calldata=_build_get_user_reserve_data_calldata,
    parse_result=parse_user_reserve_data_hex,
)
