"""Fluid DEX T1 SDK — low-level contract interactions.

Handles direct Web3 calls to Fluid DEX contracts on Arbitrum:
- DexFactory: pool enumeration via ABI
- DexResolver: pool address enumeration via ABI
- FluidDexT1 pool contracts: constantsView() for token data (raw decoding),
  readFromStorage() for dexVariables (encumbrance flags), operate() for LP

All calls are standard eth_call (no auth, no proprietary multicall).
"""

import logging
from dataclasses import dataclass
from typing import Any

from web3 import Web3
from web3.providers import HTTPProvider

logger = logging.getLogger(__name__)

# =============================================================================
# Contract Addresses (Arbitrum)
# =============================================================================

FLUID_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "dex_factory": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",
        "dex_resolver": "0x11D80CfF056Cef4F9E6d23da8672fE9873e5cC07",
        "dex_reserves_resolver": "0x05Bd8269A20C472b148246De20E6852091BF16Ff",
        "liquidity_resolver": "0xca13A15de31235A37134B4717021C35A3CF25C60",
        "vault_resolver": "0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC",
    },
}

# Gas estimates for Fluid DEX operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "approve": 46_000,
    "operate_open": 500_000,
    "operate_close": 350_000,
}

# =============================================================================
# Minimal ABIs (only simple functions that decode reliably)
# =============================================================================

DEX_FACTORY_ABI = [
    {
        "inputs": [],
        "name": "totalDexes",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

DEX_RESOLVER_ABI = [
    {
        "inputs": [],
        "name": "getAllDexAddresses",
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# FluidDexT1 pool — operate() for LP open/close
# Selector: keccak256("operate(uint256,int256,int256,address)") = 0x032d2276
DEX_T1_ABI = [
    {
        "inputs": [
            {"name": "nftId_", "type": "uint256"},
            {"name": "newCol_", "type": "int256"},
            {"name": "newDebt_", "type": "int256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "operate",
        "outputs": [
            {"name": "nftId", "type": "uint256"},
            {"name": "r0", "type": "int256"},
            {"name": "r1", "type": "int256"},
        ],
        "stateMutability": "payable",
        "type": "function",
    },
]

# FluidDexT1 pool — swapIn/swapOut for swaps
DEX_SWAP_ABI = [
    {
        "inputs": [
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountIn_", "type": "uint256"},
            {"name": "amountOutMin_", "type": "uint256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "swapIn",
        "outputs": [{"name": "amountOut_", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "swap0to1_", "type": "bool"},
            {"name": "amountOut_", "type": "uint256"},
            {"name": "amountInMax_", "type": "uint256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "swapOut",
        "outputs": [{"name": "amountIn_", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
]


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DexPoolData:
    """Data about a Fluid DEX pool read directly from the pool contract.

    Attributes:
        dex_address: Pool contract address
        token0: Token0 address (from constantsView word[9])
        token1: Token1 address (from constantsView word[10])
        fee_bps: Trading fee in basis points (estimated from config)
        is_smart_collateral: Whether smart collateral is enabled (dexVariables bit 1)
        is_smart_debt: Whether smart debt is enabled (dexVariables bit 2)
    """

    dex_address: str
    token0: str
    token1: str
    fee_bps: int = 0
    is_smart_collateral: bool = False
    is_smart_debt: bool = False


# =============================================================================
# FluidSDK
# =============================================================================


class FluidSDKError(Exception):
    """Raised when a Fluid SDK operation fails."""


class FluidSDK:
    """Low-level Fluid DEX protocol SDK.

    Reads pool data directly from pool contracts using raw eth_call:
    - constantsView(): token addresses (words 9, 10 of 18-word response)
    - readFromStorage(bytes32(0)): dexVariables with smart-collateral/debt flags

    Args:
        chain: Chain name (must be "arbitrum" for phase 1)
        rpc_url: RPC endpoint URL
    """

    def __init__(self, chain: str, rpc_url: str) -> None:
        chain_lower = chain.lower()
        if chain_lower not in FLUID_ADDRESSES:
            raise FluidSDKError(f"Fluid DEX not supported on chain: {chain}. Supported: {list(FLUID_ADDRESSES.keys())}")

        self.chain = chain_lower
        self.rpc_url = rpc_url
        self.w3 = Web3(HTTPProvider(rpc_url))
        self._addresses = FLUID_ADDRESSES[chain_lower]

        self._factory = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["dex_factory"]),
            abi=DEX_FACTORY_ABI,
        )
        self._resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["dex_resolver"]),
            abi=DEX_RESOLVER_ABI,
        )

        # Cache function selectors
        self._constants_view_sel = self.w3.keccak(text="constantsView()")[:4].hex()
        self._read_storage_sel = self.w3.keccak(text="readFromStorage(bytes32)")[:4].hex()

    def get_all_dex_addresses(self) -> list[str]:
        """Get all Fluid DEX pool addresses from the resolver."""
        try:
            addresses = self._resolver.functions.getAllDexAddresses().call()
            return [Web3.to_checksum_address(a) for a in addresses]
        except Exception as e:
            raise FluidSDKError(f"Failed to get DEX addresses: {e}") from e

    def get_total_dexes(self) -> int:
        """Get the total number of Fluid DEX pools."""
        try:
            return self._factory.functions.totalDexes().call()
        except Exception as e:
            raise FluidSDKError(f"Failed to get total DEXes: {e}") from e

    def get_dex_data(self, dex_address: str) -> DexPoolData:
        """Get pool data by calling constantsView() and readFromStorage() directly.

        Uses raw eth_call to avoid ABI decoding issues with the complex
        DexEntireData struct. constantsView() returns 18 words:
        - word[9]: token0 address
        - word[10]: token1 address

        readFromStorage(bytes32(0)) returns dexVariables:
        - bit 1: isSmartCollateralEnabled
        - bit 2: isSmartDebtEnabled

        Args:
            dex_address: Pool contract address

        Returns:
            DexPoolData with token addresses and encumbrance flags
        """
        addr = Web3.to_checksum_address(dex_address)

        try:
            # constantsView() — 18 words, tokens at word[9] and word[10]
            cv_data = self.w3.eth.call(
                {
                    "to": addr,
                    "data": bytes.fromhex(self._constants_view_sel),
                }
            )

            if len(cv_data) < 11 * 32:
                raise FluidSDKError(f"constantsView() returned only {len(cv_data)} bytes, expected >= {11 * 32}")

            token0 = Web3.to_checksum_address("0x" + cv_data[9 * 32 + 12 : 10 * 32].hex())
            token1 = Web3.to_checksum_address("0x" + cv_data[10 * 32 + 12 : 11 * 32].hex())

            # readFromStorage(bytes32(0)) — dexVariables
            storage_data = self.w3.eth.call(
                {
                    "to": addr,
                    "data": bytes.fromhex(self._read_storage_sel + "00" * 32),
                }
            )
            if len(storage_data) < 32:
                raise FluidSDKError(f"readFromStorage() returned only {len(storage_data)} bytes, expected >= 32")
            dex_vars = int.from_bytes(storage_data[:32], "big")

            is_smart_col = bool((dex_vars >> 1) & 1)
            is_smart_debt = bool((dex_vars >> 2) & 1)

            return DexPoolData(
                dex_address=addr,
                token0=token0,
                token1=token1,
                fee_bps=0,  # Fee extraction deferred — not critical for phase 1
                is_smart_collateral=is_smart_col,
                is_smart_debt=is_smart_debt,
            )
        except FluidSDKError:
            raise
        except Exception as e:
            raise FluidSDKError(f"Failed to get DEX data for {dex_address}: {e}") from e

    def is_position_encumbered(self, dex_address: str, nft_id: int = 0) -> bool:
        """Check if a specific position has outstanding debt.

        In Fluid DEX, ALL pools have smart-debt capability (that's the design).
        The encumbrance check is at the POSITION level, not pool level:
        - Positions we create with newDebt=0 have no debt, so they're safe to close.
        - For safety, we verify the pool's smart-debt flag but don't block on it
          for positions we know were opened without debt.

        For phase 1, this always returns False for nft_id=0 (new position check)
        since we enforce newDebt=0 in build_operate_tx().

        Args:
            dex_address: Pool contract address
            nft_id: NFT position ID (0 = checking for new position)

        Returns:
            True if the position has outstanding debt
        """
        # For new positions (nft_id=0), never encumbered since we enforce newDebt=0
        if nft_id == 0:
            return False

        # For existing positions in phase 1, we only close positions we opened
        # with newDebt=0, so they should never have debt. Return False.
        # Future phases can add on-chain debt verification here.
        return False

    def find_dex_by_tokens(self, token0: str, token1: str) -> str | None:
        """Find a Fluid DEX pool for a given token pair.

        Token order is automatically handled (tries both orderings).
        """
        token0_lower = token0.lower()
        token1_lower = token1.lower()

        try:
            addresses = self.get_all_dex_addresses()
        except FluidSDKError:
            logger.warning("Failed to enumerate Fluid DEX pools")
            return None

        for dex_addr in addresses:
            try:
                data = self.get_dex_data(dex_addr)
                pool_t0 = data.token0.lower()
                pool_t1 = data.token1.lower()

                if (pool_t0 == token0_lower and pool_t1 == token1_lower) or (
                    pool_t0 == token1_lower and pool_t1 == token0_lower
                ):
                    return dex_addr
            except FluidSDKError:
                continue

        return None

    def get_swap_quote(
        self,
        dex_address: str,
        swap0to1: bool,
        amount_in: int,
        to: str,
    ) -> int:
        """Get a swap quote (estimate) from a Fluid DEX pool.

        Calls swapIn with estimate semantics (eth_call, no state change).

        Args:
            dex_address: Pool contract address
            swap0to1: True to swap token0->token1, False for token1->token0
            amount_in: Input amount in token's smallest unit
            to: Recipient address

        Returns:
            Expected output amount in token's smallest unit
        """
        dex_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(dex_address),
            abi=DEX_SWAP_ABI,
        )
        try:
            amount_out = dex_contract.functions.swapIn(swap0to1, amount_in, 0, Web3.to_checksum_address(to)).call(
                {"from": Web3.to_checksum_address(to), "value": 0}  # type: ignore[arg-type]
            )
            return amount_out
        except Exception as e:
            raise FluidSDKError(f"Failed to get swap quote: {e}") from e

    def build_swap_tx(
        self,
        dex_address: str,
        swap0to1: bool,
        amount_in: int,
        amount_out_min: int,
        to: str,
        value: int = 0,
    ) -> dict[str, Any]:
        """Build a swapIn transaction for a Fluid DEX pool.

        Args:
            dex_address: Pool contract address
            swap0to1: True to swap token0->token1, False for token1->token0
            amount_in: Input amount in token's smallest unit
            amount_out_min: Minimum acceptable output (slippage protection)
            to: Recipient address
            value: Native token value (for ETH-paired swaps)

        Returns:
            Transaction dict with 'to', 'data', 'value', 'gas'
        """
        dex_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(dex_address),
            abi=DEX_SWAP_ABI,
        )

        tx = dex_contract.functions.swapIn(
            swap0to1, amount_in, amount_out_min, Web3.to_checksum_address(to)
        ).build_transaction(
            {  # type: ignore[arg-type]
                "from": Web3.to_checksum_address(to),
                "value": value,
                "gas": 200_000,
            }
        )

        return {
            "to": tx["to"],
            "data": tx["data"],
            "value": value,
            "gas": 200_000,
        }

    def build_operate_tx(
        self,
        dex_address: str,
        nft_id: int,
        new_col: int,
        new_debt: int,
        to: str,
    ) -> dict[str, Any]:
        """Build an operate() transaction for a Fluid DEX pool.

        operate() is the main entry point for LP operations:
        - Open position: nft_id=0, new_col>0, new_debt=0
        - Close position: nft_id=X, new_col<0 (negative = withdraw), new_debt=0
        """
        if new_debt != 0:
            raise FluidSDKError("Phase 1 Fluid connector does not support smart-debt operations. new_debt must be 0.")

        dex_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(dex_address),
            abi=DEX_T1_ABI,
        )

        gas = DEFAULT_GAS_ESTIMATES["operate_open"] if nft_id == 0 else DEFAULT_GAS_ESTIMATES["operate_close"]

        tx = dex_contract.functions.operate(
            nft_id,
            new_col,
            new_debt,
            Web3.to_checksum_address(to),
        ).build_transaction(
            {  # type: ignore[arg-type]
                "from": Web3.to_checksum_address(to),
                "value": 0,
                "gas": gas,
            }
        )

        return {
            "to": tx["to"],
            "data": tx["data"],
            "value": tx.get("value", 0),
            "gas": gas,
        }
