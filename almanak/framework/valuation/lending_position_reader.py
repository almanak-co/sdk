"""On-chain Aave V3 lending position reader via gateway RPC.

Queries the Aave V3 PoolDataProvider's getUserReserveData for each asset
to get current supply (aToken balance) and debt (stable + variable).

Uses the gateway's generic Call RPC -- no proto changes needed.
Same pattern as lp_position_reader.py.
"""

import json
import logging
from dataclasses import dataclass

from almanak.core.contracts import AAVE_V3

logger = logging.getLogger(__name__)

# Aave V3 Pool Data Provider addresses — derived from the centralized registry
# in almanak/core/contracts.py (single source of truth for all Aave V3 addresses).
AAVE_V3_POOL_DATA_PROVIDER: dict[str, str] = {chain: addrs["pool_data_provider"] for chain, addrs in AAVE_V3.items()}

# Function selector for getUserReserveData(address asset, address user)
GET_USER_RESERVE_DATA_SELECTOR = "0x28dd2d01"


@dataclass
class LendingPositionOnChain:
    """On-chain state of an Aave V3 lending position for a single asset.

    Decoded from PoolDataProvider.getUserReserveData(asset, user).
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


class LendingPositionReader:
    """Reads Aave V3 lending positions via gateway RPC.

    Queries getUserReserveData for specified assets and decodes
    the ABI-encoded response client-side.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def read_position(
        self,
        chain: str,
        asset_address: str,
        wallet_address: str,
    ) -> LendingPositionOnChain | None:
        """Query a single asset's lending position for a wallet.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            asset_address: Underlying asset contract address
            wallet_address: User wallet address

        Returns:
            LendingPositionOnChain with supply/debt data, or None on failure.
        """
        if self._gateway is None:
            return None

        data_provider = AAVE_V3_POOL_DATA_PROVIDER.get(chain)
        if not data_provider:
            logger.debug("No Aave V3 data provider for chain %s", chain)
            return None

        # Build calldata: getUserReserveData(address asset, address user)
        asset_padded = _pad_address(asset_address)
        wallet_padded = _pad_address(wallet_address)
        calldata = GET_USER_RESERVE_DATA_SELECTOR + asset_padded + wallet_padded

        result_hex = self._eth_call(chain, data_provider, calldata)
        if not result_hex:
            return None

        return _parse_user_reserve_data_hex(result_hex, asset_address)

    def read_positions(
        self,
        chain: str,
        asset_addresses: list[str],
        wallet_address: str,
    ) -> list[LendingPositionOnChain]:
        """Query multiple assets' lending positions for a wallet.

        Returns only active positions (non-zero supply or debt).

        Args:
            chain: Chain identifier
            asset_addresses: List of underlying asset addresses to check
            wallet_address: User wallet address

        Returns:
            List of active LendingPositionOnChain entries.
        """
        positions = []
        for asset in asset_addresses:
            pos = self.read_position(chain, asset, wallet_address)
            if pos is not None and pos.is_active:
                positions.append(pos)
        return positions

    def _eth_call(self, chain: str, to: str, data: str) -> str | None:
        """Make an eth_call via gateway generic RPC."""
        try:
            from almanak.gateway.proto import gateway_pb2

            rpc_stub = getattr(self._gateway, "_rpc_stub", None)
            if rpc_stub is None:
                logger.debug("Gateway client not connected for lending position query")
                return None

            timeout = getattr(getattr(self._gateway, "config", None), "timeout", 10)

            params_json = json.dumps([{"to": to, "data": data}, "latest"])
            response = rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=params_json,
                ),
                timeout=timeout,
            )

            if not response.success:
                logger.debug("eth_call failed for lending position: %s", response.error)
                return None

            if response.result:
                return json.loads(response.result)
            return None
        except Exception:
            logger.debug("Failed to make eth_call for lending position", exc_info=True)
            return None


def _pad_address(address: str) -> str:
    """Left-pad an address to 32 bytes (64 hex chars)."""
    addr = address.lower().replace("0x", "")
    return addr.zfill(64)


def _parse_user_reserve_data_hex(
    hex_data: str,
    asset_address: str,
) -> LendingPositionOnChain | None:
    """Parse hex response from getUserReserveData.

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
    # Strip 0x prefix
    data = hex_data.replace("0x", "")

    # 9 words * 64 hex chars = 576 minimum
    if len(data) < 576:
        logger.warning("Aave getUserReserveData response too short: %d chars", len(data))
        return None

    try:
        atoken_balance = _decode_uint_hex(data, 0)
        stable_debt = _decode_uint_hex(data, 1)
        variable_debt = _decode_uint_hex(data, 2)
        liquidity_rate = _decode_uint_hex(data, 6)
        collateral_enabled = _decode_uint_hex(data, 8) != 0

        return LendingPositionOnChain(
            asset_address=asset_address,
            current_atoken_balance=atoken_balance,
            current_stable_debt=stable_debt,
            current_variable_debt=variable_debt,
            liquidity_rate=liquidity_rate,
            usage_as_collateral_enabled=collateral_enabled,
        )
    except Exception:
        logger.debug("Failed to parse Aave user reserve data hex", exc_info=True)
        return None


def _decode_uint_hex(hex_data: str, word_index: int) -> int:
    """Decode a uint256 from ABI-encoded hex at the given word index."""
    start = word_index * 64
    return int(hex_data[start : start + 64], 16)
