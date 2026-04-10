"""Euler V2 lending protocol adapter for Avalanche.

Euler V2 uses an ERC-4626 vault architecture where each vault holds a single
underlying ERC-20 asset. Cross-vault borrowing is coordinated through the
Ethereum Vault Connector (EVC), which enables collateral/controller relationships
between vaults.

Key architecture:
- Individual ERC-4626 vaults per asset (not shared pools)
- EVC (Ethereum Vault Connector) for cross-vault collateral/borrow relationships
- Permissionless vault deployment via factory
- Sub-accounts (256 per address) for isolated position management
- Deferred solvency checks within EVC batches

Supported operations: SUPPLY, WITHDRAW, BORROW, REPAY
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

# =============================================================================
# Contract Addresses (Avalanche C-Chain)
# =============================================================================

# Ethereum Vault Connector — coordinates cross-vault collateral/borrow
EVC_ADDRESS = "0xddcbe30A761Edd2e19bba930A977475265F36Fa1"

# eVault Factory — creates new vaults (247 deployed as of 2026-04)
EVAULT_FACTORY_ADDRESS = "0xaf4B4c18B17F6a2B32F6c398a3910bdCD7f26181"

# Vault Lens — read-only batch queries
VAULT_LENS_ADDRESS = "0x7a2A57a0ed6807c7dbF846cc74aa04eE9DFa7F57"

# =============================================================================
# Function Selectors (from `cast sig`)
# =============================================================================

# ERC-4626 vault operations
DEPOSIT_SELECTOR = "0x6e553f65"  # deposit(uint256,address)
WITHDRAW_SELECTOR = "0xb460af94"  # withdraw(uint256,address,address)
REDEEM_SELECTOR = "0xba087652"  # redeem(uint256,address,address)

# Euler V2 borrow/repay
BORROW_SELECTOR = "0x4b3fd148"  # borrow(uint256,address)
REPAY_SELECTOR = "0xacb70815"  # repay(uint256,address)

# EVC operations
ENABLE_COLLATERAL_SELECTOR = "0xd44fee5a"  # enableCollateral(address,address)
ENABLE_CONTROLLER_SELECTOR = "0xc368516c"  # enableController(address,address)

# View functions
ASSET_SELECTOR = "0x38d52e0f"  # asset()
TOTAL_ASSETS_SELECTOR = "0x01e1d114"  # totalAssets()
CASH_SELECTOR = "0x961be391"  # cash()
DEBT_OF_SELECTOR = "0xd283e75f"  # debtOf(address)
MAX_WITHDRAW_SELECTOR = "0xce96cb77"  # maxWithdraw(address)

MAX_UINT256 = 2**256 - 1

# =============================================================================
# Gas Estimates
# =============================================================================

GAS_ESTIMATE_SUPPLY = 250_000
GAS_ESTIMATE_WITHDRAW = 300_000
GAS_ESTIMATE_BORROW = 500_000  # Higher due to EVC batch operations
GAS_ESTIMATE_REPAY = 250_000
GAS_ESTIMATE_APPROVE = 50_000

# =============================================================================
# Euler V2 Vault Registry (Avalanche)
# =============================================================================

# Map of vault_symbol -> {address, underlying_symbol, underlying_address, decimals}
# Curated list of active vaults with meaningful liquidity
EULER_V2_VAULTS: dict[str, dict] = {
    "eUSDC-19": {
        "vault_address": "0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e",
        "underlying_symbol": "USDC",
        "underlying_address": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "decimals": 6,
        "preferred": True,  # Re7Labs vault, generally highest TVL
    },
    "eUSDC-2": {
        "vault_address": "0x39dE0f00189306062D79eDEC6DcA5bb6bFd108f9",
        "underlying_symbol": "USDC",
        "underlying_address": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "decimals": 6,
        "preferred": False,
    },
    "eUSDC-15": {
        "vault_address": "0x8f23Da78e3F31Ab5DEb75dC3282198bed630ffde",
        "underlying_symbol": "USDC",
        "underlying_address": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "decimals": 6,
        "preferred": False,
    },
    "eWAVAX-2": {
        "vault_address": "0x6c718a70239fA548c0bD268fE88F37EBE8b6E2ea",
        "underlying_symbol": "WAVAX",
        "underlying_address": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "decimals": 18,
        "preferred": True,
    },
    "eUSDt-3": {
        "vault_address": "0xa446938b0204Aa4055cdFEd68Ddf0E0d1BAB3E9E",
        "underlying_symbol": "USDT",
        "underlying_address": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "decimals": 6,
        "preferred": True,
    },
    "eAUSD-2": {
        "vault_address": "0x2137568666f12fc5A026f5430Ae7194F1C1362aB",
        "underlying_symbol": "AUSD",
        "underlying_address": "0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a",
        "decimals": 6,
        "preferred": True,
    },
    "eBTC.b-33": {
        "vault_address": "0xF983f92bd962A94EAc85a8c58237C1CC1cDfBBBa",
        "underlying_symbol": "BTC.b",
        "underlying_address": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "decimals": 8,
        "preferred": True,
    },
    "esAVAX-2": {
        "vault_address": "0x38a559c2b6eF3fF7Cdc40a800D6351a2B70b2243",
        "underlying_symbol": "sAVAX",
        "underlying_address": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
        "decimals": 18,
        "preferred": True,
    },
}

# Reverse lookup: underlying_symbol -> list of vault symbols (sorted by preference)
_TOKEN_TO_VAULT_MAP: dict[str, list[str]] = {}
for _vault_sym, _vault_info in EULER_V2_VAULTS.items():
    _underlying = _vault_info["underlying_symbol"]
    if _underlying not in _TOKEN_TO_VAULT_MAP:
        _TOKEN_TO_VAULT_MAP[_underlying] = []
    _TOKEN_TO_VAULT_MAP[_underlying].append(_vault_sym)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class EulerV2Config:
    """Configuration for EulerV2Adapter."""

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50

    def __post_init__(self) -> None:
        if self.chain != "avalanche":
            raise ValueError(f"Euler V2 connector only supports Avalanche, got: {self.chain}")
        if not self.wallet_address.startswith("0x"):
            raise ValueError(f"Invalid wallet address: {self.wallet_address}")


@dataclass
class EulerV2VaultInfo:
    """Information about an Euler V2 vault."""

    vault_symbol: str
    vault_address: str
    underlying_symbol: str
    underlying_address: str
    decimals: int


@dataclass
class TransactionResult:
    """Result of building a transaction."""

    success: bool
    tx_data: dict | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None


# =============================================================================
# Adapter
# =============================================================================


class EulerV2Adapter:
    """Adapter for Euler V2 lending protocol on Avalanche.

    Builds raw transaction data for supply, withdraw, borrow, and repay
    operations against Euler V2 ERC-4626 vaults.
    """

    def __init__(self, config: EulerV2Config) -> None:
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.evc_address = EVC_ADDRESS

    # =========================================================================
    # Vault Resolution
    # =========================================================================

    def find_vault_for_asset(
        self,
        asset_symbol: str,
        vault_symbol: str | None = None,
    ) -> EulerV2VaultInfo | None:
        """Find an Euler V2 vault for a given asset.

        Args:
            asset_symbol: Token symbol (e.g., "USDC", "WAVAX")
            vault_symbol: Optional specific vault symbol (e.g., "eUSDC-19")

        Returns:
            EulerV2VaultInfo or None if not found
        """
        if vault_symbol and vault_symbol in EULER_V2_VAULTS:
            info = EULER_V2_VAULTS[vault_symbol]
            return EulerV2VaultInfo(
                vault_symbol=vault_symbol,
                vault_address=info["vault_address"],
                underlying_symbol=info["underlying_symbol"],
                underlying_address=info["underlying_address"],
                decimals=info["decimals"],
            )

        asset_upper = asset_symbol.upper()
        vault_symbols = _TOKEN_TO_VAULT_MAP.get(asset_upper, [])
        if not vault_symbols:
            return None

        # Prefer vaults marked as preferred, fall back to first in list
        sym = next(
            (s for s in vault_symbols if EULER_V2_VAULTS[s].get("preferred", False)),
            vault_symbols[0],
        )
        info = EULER_V2_VAULTS[sym]
        return EulerV2VaultInfo(
            vault_symbol=sym,
            vault_address=info["vault_address"],
            underlying_symbol=info["underlying_symbol"],
            underlying_address=info["underlying_address"],
            decimals=info["decimals"],
        )

    def get_supported_assets(self) -> list[str]:
        """Return list of supported underlying asset symbols."""
        return list(_TOKEN_TO_VAULT_MAP.keys())

    # =========================================================================
    # Supply (ERC-4626 deposit)
    # =========================================================================

    def supply(
        self,
        asset: str,
        amount: Decimal,
        vault_symbol: str | None = None,
    ) -> TransactionResult:
        """Build a deposit transaction for an Euler V2 vault.

        Uses ERC-4626 deposit(uint256 amount, address receiver).

        Args:
            asset: Token symbol to deposit (e.g., "USDC")
            amount: Amount in human-readable units
            vault_symbol: Optional specific vault symbol

        Returns:
            TransactionResult with tx_data for deposit
        """
        vault = self.find_vault_for_asset(asset, vault_symbol)
        if not vault:
            return TransactionResult(
                success=False,
                error=f"No Euler V2 vault found for asset: {asset}",
            )

        amount_wei = int(amount * Decimal(10**vault.decimals))

        # deposit(uint256 amount, address receiver)
        calldata = DEPOSIT_SELECTOR + _encode_uint256(amount_wei) + _encode_address(self.wallet_address)

        return TransactionResult(
            success=True,
            tx_data={
                "to": vault.vault_address,
                "value": 0,
                "data": calldata,
            },
            gas_estimate=GAS_ESTIMATE_SUPPLY,
            description=f"Deposit {amount} {asset} into Euler V2 vault {vault.vault_symbol}",
        )

    # =========================================================================
    # Withdraw (ERC-4626 withdraw)
    # =========================================================================

    def withdraw(
        self,
        asset: str,
        amount: Decimal,
        withdraw_all: bool = False,
        vault_symbol: str | None = None,
    ) -> TransactionResult:
        """Build a withdraw transaction for an Euler V2 vault.

        Uses ERC-4626 withdraw(uint256 assets, address receiver, address owner)
        for specific amounts, or redeem(uint256 shares, address receiver, address owner)
        with MAX_UINT256 for withdraw_all.

        Args:
            asset: Token symbol to withdraw
            amount: Amount in human-readable units (ignored if withdraw_all=True)
            withdraw_all: If True, redeem all shares
            vault_symbol: Optional specific vault symbol

        Returns:
            TransactionResult with tx_data for withdraw
        """
        vault = self.find_vault_for_asset(asset, vault_symbol)
        if not vault:
            return TransactionResult(
                success=False,
                error=f"No Euler V2 vault found for asset: {asset}",
            )

        if withdraw_all:
            # redeem(uint256 shares, address receiver, address owner) with MAX_UINT256.
            # Euler V2 EVault inherits OpenZeppelin's ERC4626 which handles
            # type(uint256).max in redeem() by capping to balanceOf(owner).
            # This is safe and standard behavior for OZ-based ERC4626 vaults.
            calldata = (
                REDEEM_SELECTOR
                + _encode_uint256(MAX_UINT256)
                + _encode_address(self.wallet_address)
                + _encode_address(self.wallet_address)
            )
            description = f"Withdraw all {asset} from Euler V2 vault {vault.vault_symbol}"
        else:
            amount_wei = int(amount * Decimal(10**vault.decimals))
            # withdraw(uint256 assets, address receiver, address owner)
            calldata = (
                WITHDRAW_SELECTOR
                + _encode_uint256(amount_wei)
                + _encode_address(self.wallet_address)
                + _encode_address(self.wallet_address)
            )
            description = f"Withdraw {amount} {asset} from Euler V2 vault {vault.vault_symbol}"

        return TransactionResult(
            success=True,
            tx_data={
                "to": vault.vault_address,
                "value": 0,
                "data": calldata,
            },
            gas_estimate=GAS_ESTIMATE_WITHDRAW,
            description=description,
        )

    # =========================================================================
    # Borrow (requires EVC: enableCollateral + enableController + borrow)
    # =========================================================================

    def borrow(
        self,
        borrow_asset: str,
        borrow_amount: Decimal,
        collateral_vault_address: str | None = None,
        borrow_vault_symbol: str | None = None,
    ) -> TransactionResult:
        """Build a borrow transaction for Euler V2.

        Borrowing requires EVC setup:
        1. enableCollateral(account, collateralVault) — register collateral
        2. enableController(account, borrowVault) — grant borrow vault access
        3. borrow(amount, receiver) — borrow from the vault

        These are batched via EVC.batch() for atomicity.

        Args:
            borrow_asset: Token symbol to borrow
            borrow_amount: Amount in human-readable units
            collateral_vault_address: Address of the collateral vault (must already have deposits)
            borrow_vault_symbol: Optional specific borrow vault symbol

        Returns:
            TransactionResult with tx_data for EVC batch
        """
        borrow_vault = self.find_vault_for_asset(borrow_asset, borrow_vault_symbol)
        if not borrow_vault:
            return TransactionResult(
                success=False,
                error=f"No Euler V2 vault found for borrow asset: {borrow_asset}",
            )

        if not collateral_vault_address:
            return TransactionResult(
                success=False,
                error="collateral_vault_address is required for Euler V2 borrow",
            )

        borrow_amount_wei = int(borrow_amount * Decimal(10**borrow_vault.decimals))

        # Build EVC batch items:
        # 1. enableCollateral(account, collateralVault)
        enable_collateral_data = (
            ENABLE_COLLATERAL_SELECTOR
            + _encode_address(self.wallet_address)
            + _encode_address(collateral_vault_address)
        )

        # 2. enableController(account, borrowVault)
        enable_controller_data = (
            ENABLE_CONTROLLER_SELECTOR
            + _encode_address(self.wallet_address)
            + _encode_address(borrow_vault.vault_address)
        )

        # 3. borrow(amount, receiver) — called on the borrow vault via EVC
        borrow_data = BORROW_SELECTOR + _encode_uint256(borrow_amount_wei) + _encode_address(self.wallet_address)

        # Encode EVC batch call
        # batch(BatchItem[]) where BatchItem = (address targetContract, address onBehalfOfAccount, uint256 value, bytes data)
        batch_calldata = _encode_evc_batch(
            [
                (self.evc_address, self.wallet_address, 0, bytes.fromhex(enable_collateral_data[2:])),
                (self.evc_address, self.wallet_address, 0, bytes.fromhex(enable_controller_data[2:])),
                (borrow_vault.vault_address, self.wallet_address, 0, bytes.fromhex(borrow_data[2:])),
            ]
        )

        return TransactionResult(
            success=True,
            tx_data={
                "to": self.evc_address,
                "value": 0,
                "data": "0x" + batch_calldata.hex(),
            },
            gas_estimate=GAS_ESTIMATE_BORROW,
            description=f"Borrow {borrow_amount} {borrow_asset} from Euler V2 (EVC batch: enableCollateral + enableController + borrow)",
        )

    # =========================================================================
    # Repay
    # =========================================================================

    def repay(
        self,
        asset: str,
        amount: Decimal,
        repay_all: bool = False,
        vault_symbol: str | None = None,
    ) -> TransactionResult:
        """Build a repay transaction for Euler V2.

        Uses repay(uint256 amount, address receiver).
        For full repay, uses type(uint256).max which repays entire debt.

        Args:
            asset: Token symbol to repay
            amount: Amount in human-readable units (ignored if repay_all=True)
            repay_all: If True, repay full debt using MAX_UINT256
            vault_symbol: Optional specific vault symbol

        Returns:
            TransactionResult with tx_data for repay
        """
        vault = self.find_vault_for_asset(asset, vault_symbol)
        if not vault:
            return TransactionResult(
                success=False,
                error=f"No Euler V2 vault found for asset: {asset}",
            )

        if repay_all:
            repay_amount_wei = MAX_UINT256
            description = f"Repay all {asset} debt on Euler V2 vault {vault.vault_symbol}"
        else:
            repay_amount_wei = int(amount * Decimal(10**vault.decimals))
            description = f"Repay {amount} {asset} on Euler V2 vault {vault.vault_symbol}"

        # repay(uint256 amount, address receiver)
        calldata = REPAY_SELECTOR + _encode_uint256(repay_amount_wei) + _encode_address(self.wallet_address)

        return TransactionResult(
            success=True,
            tx_data={
                "to": vault.vault_address,
                "value": 0,
                "data": calldata,
            },
            gas_estimate=GAS_ESTIMATE_REPAY,
            description=description,
        )


# =============================================================================
# ABI Encoding Helpers
# =============================================================================


def _encode_uint256(value: int) -> str:
    """Encode a uint256 value as a 0x-prefixed hex string (64 chars, no 0x prefix in output)."""
    return format(value, "064x")


def _encode_address(address: str) -> str:
    """Encode an address as a 32-byte left-padded hex string (no 0x prefix in output)."""
    addr = address.lower().replace("0x", "")
    return addr.zfill(64)


def _encode_evc_batch(items: list[tuple[str, str, int, bytes]]) -> bytes:
    """Encode an EVC batch(BatchItem[]) call using eth_abi.

    Each item is (targetContract, onBehalfOfAccount, value, data).

    batch(BatchItem[]) where:
        struct BatchItem {
            address targetContract;
            address onBehalfOfAccount;
            uint256 value;
            bytes data;
        }
    """
    from eth_abi import encode
    from web3 import Web3

    batch_selector = Web3.keccak(text="batch((address,address,uint256,bytes)[])")[:4]

    # Convert addresses to checksummed format for eth_abi
    batch_items = [
        (Web3.to_checksum_address(target), Web3.to_checksum_address(on_behalf), value, data)
        for target, on_behalf, value, data in items
    ]

    encoded = encode(["(address,address,uint256,bytes)[]"], [batch_items])

    return batch_selector + encoded
