"""Euler V2 lending protocol adapter (Avalanche + Ethereum).

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

Supported chains: Avalanche, Ethereum
Supported operations: SUPPLY, WITHDRAW, BORROW, REPAY
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

# =============================================================================
# Contract Addresses (per chain)
# =============================================================================

# Supported chains and their core addresses
CHAIN_ADDRESSES: dict[str, dict[str, str]] = {
    "avalanche": {
        "evc": "0xddcbe30A761Edd2e19bba930A977475265F36Fa1",
        "evault_factory": "0xaf4B4c18B17F6a2B32F6c398a3910bdCD7f26181",
        "vault_lens": "0x7a2A57a0ed6807c7dbF846cc74aa04eE9DFa7F57",
    },
    "ethereum": {
        "evc": "0x0C9a3dd6b8F28529d72d7f9cE918D493519EE383",
        "evault_factory": "0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e",
        "vault_lens": "0xA18D79deB85C414989D7297F23e5391703Ea66aB",
    },
    # Core addresses from euler-xyz/euler-interfaces EulerChains.json (status:production),
    # cross-checked against the ethereum/avalanche values above. Verified 2026-06-26.
    # See docs/internal/euler-v2-chain-extension-research-20260626.md.
    "base": {
        "evc": "0x5301c7dD20bD945D2013b48ed0DEE3A284ca8989",
        "evault_factory": "0x7F321498A801A191a93C840750ed637149dDf8D0",
        "vault_lens": "0x601F023CD063324DdbCADa69460e969fb97e98b9",
    },
    "arbitrum": {
        "evc": "0x6302ef0F34100CDDFb5489fbcB6eE1AA95CD1066",
        "evault_factory": "0x78Df1CF5bf06a7f27f2ACc580B934238C1b80D50",
        "vault_lens": "0x8E0321a0f6d37411136077215ED9A539C1B16258",
    },
}

SUPPORTED_CHAINS = set(CHAIN_ADDRESSES.keys())

# Legacy aliases for backward compatibility
EVC_ADDRESS = CHAIN_ADDRESSES["avalanche"]["evc"]
EVAULT_FACTORY_ADDRESS = CHAIN_ADDRESSES["avalanche"]["evault_factory"]
VAULT_LENS_ADDRESS = CHAIN_ADDRESSES["avalanche"]["vault_lens"]

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
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# =============================================================================
# Gas Estimates
# =============================================================================

GAS_ESTIMATE_SUPPLY = 250_000
GAS_ESTIMATE_WITHDRAW = 300_000
GAS_ESTIMATE_BORROW = 500_000  # Higher due to EVC batch operations
GAS_ESTIMATE_REPAY = 250_000
GAS_ESTIMATE_APPROVE = 50_000

# =============================================================================
# Euler V2 Vault Registry (per chain)
# =============================================================================

# Map of vault_symbol -> {address, underlying_symbol, underlying_address, decimals}
# Curated list of active vaults with meaningful liquidity
_AVALANCHE_VAULTS: dict[str, dict] = {
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
    # esAVAX-32 (live, governed). Replaces the defunct esAVAX-2 (0x38a559c2…,
    # maxDeposit=0). Enabled as collateral on the eUSDC-19 borrow vault at 70% LTV
    # (LTVBorrow != 0), verified on-chain 2026-06-26 — this is the collateral leg
    # the avalanche borrow/repay path needs (the "no registered collateral vault"
    # gap documented in tests/intents/avalanche/test_euler_v2_borrow.py since
    # 2026-04-10). See docs/internal/euler-v2-chain-extension-research-20260626.md.
    "esAVAX-32": {
        "vault_address": "0xf3aCc3Fc22E376fa3dD21CF883B60DDE9cf4E34f",
        "underlying_symbol": "sAVAX",
        "underlying_address": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
        "decimals": 18,
        "preferred": True,
    },
}

_ETHEREUM_VAULTS: dict[str, dict] = {
    # eWETH-2 collateral, enabled on eUSDC-2 at 84% LTV (verified on-chain 2026-06-26
    # via governedPerspective + LTVBorrow). Highest-TVL governed eWETH vault.
    "eWETH-2": {
        "vault_address": "0xD8b27CF359b7D15710a5BE299AF6e7Bf904984C2",
        "underlying_symbol": "WETH",
        "underlying_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "decimals": 18,
        "preferred": True,
    },
    "eUSDC-2": {
        "vault_address": "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9",
        "underlying_symbol": "USDC",
        "underlying_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
        "preferred": True,
    },
}

# Base (chain 8453). Borrow-capable pair verified on-chain 2026-06-26: eWETH-1 is
# enabled as collateral on eUSDC-1 at 86% LTV (governedPerspective + LTVBorrow). Note
# the borrow-capable USDC vault (eUSDC-1, ~$325K) differs from the max-TVL supply vault
# (eUSDC-86); for the single-preferred-per-underlying model we pick the borrow-capable
# one, which also has ample supply liquidity. See the chain-extension research doc.
_BASE_VAULTS: dict[str, dict] = {
    "eWETH-1": {
        "vault_address": "0x859160DB5841E5cfB8D3f144C6b3381A85A4b410",
        "underlying_symbol": "WETH",
        "underlying_address": "0x4200000000000000000000000000000000000006",
        "decimals": 18,
        "preferred": True,
    },
    "eUSDC-1": {
        "vault_address": "0x0A1a3b5f2041F33522C4efc754a7D096f880eE16",
        "underlying_symbol": "USDC",
        "underlying_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "decimals": 6,
        "preferred": True,
    },
}

# Arbitrum (chain 42161). Borrow-capable pair verified on-chain 2026-06-26: eWETH-1
# enabled as collateral on eUSDC-1 at 85% LTV. eUSDC-1 (~$356K) is the borrow-capable
# USDC vault (vs the max-TVL eUSDC-2).
_ARBITRUM_VAULTS: dict[str, dict] = {
    "eWETH-1": {
        "vault_address": "0x78E3E051D32157AACD550fBB78458762d8f7edFF",
        "underlying_symbol": "WETH",
        "underlying_address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "decimals": 18,
        "preferred": True,
    },
    "eUSDC-1": {
        "vault_address": "0x0a1eCC5Fe8C9be3C809844fcBe615B46A869b899",
        "underlying_symbol": "USDC",
        "underlying_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "decimals": 6,
        "preferred": True,
    },
}

# Per-chain vault registries
EULER_V2_VAULTS_BY_CHAIN: dict[str, dict[str, dict]] = {
    "avalanche": _AVALANCHE_VAULTS,
    "ethereum": _ETHEREUM_VAULTS,
    "base": _BASE_VAULTS,
    "arbitrum": _ARBITRUM_VAULTS,
}

# Legacy alias: flat dict for backward compatibility (Avalanche only)
EULER_V2_VAULTS = _AVALANCHE_VAULTS

# Per-chain reverse lookup: chain -> (underlying_symbol (UPPER) -> list of vault symbols)
_TOKEN_TO_VAULT_MAP_BY_CHAIN: dict[str, dict[str, list[str]]] = {}
for _chain_name, _chain_vaults in EULER_V2_VAULTS_BY_CHAIN.items():
    _chain_map: dict[str, list[str]] = {}
    for _vault_sym, _vault_info in _chain_vaults.items():
        _underlying = _vault_info["underlying_symbol"].upper()
        if _underlying not in _chain_map:
            _chain_map[_underlying] = []
        _chain_map[_underlying].append(_vault_sym)
    _TOKEN_TO_VAULT_MAP_BY_CHAIN[_chain_name] = _chain_map

# Legacy alias for backward compatibility
_TOKEN_TO_VAULT_MAP = _TOKEN_TO_VAULT_MAP_BY_CHAIN.get("avalanche", {})


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
        if self.chain not in SUPPORTED_CHAINS:
            raise ValueError(f"Euler V2 connector supports {', '.join(sorted(SUPPORTED_CHAINS))}, got: {self.chain}")
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
        chain_addrs = CHAIN_ADDRESSES[self.chain]
        self.evc_address = chain_addrs["evc"]
        self._vaults = EULER_V2_VAULTS_BY_CHAIN.get(self.chain, {})
        self._token_to_vault_map = _TOKEN_TO_VAULT_MAP_BY_CHAIN.get(self.chain, {})

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
        if vault_symbol and vault_symbol in self._vaults:
            info = self._vaults[vault_symbol]
            return EulerV2VaultInfo(
                vault_symbol=vault_symbol,
                vault_address=info["vault_address"],
                underlying_symbol=info["underlying_symbol"],
                underlying_address=info["underlying_address"],
                decimals=info["decimals"],
            )

        asset_upper = asset_symbol.upper()
        vault_symbols = self._token_to_vault_map.get(asset_upper, [])
        if not vault_symbols:
            return None

        # Prefer vaults marked as preferred, fall back to first in list
        sym = next(
            (s for s in vault_symbols if self._vaults[s].get("preferred", False)),
            vault_symbols[0],
        )
        info = self._vaults[sym]
        return EulerV2VaultInfo(
            vault_symbol=sym,
            vault_address=info["vault_address"],
            underlying_symbol=info["underlying_symbol"],
            underlying_address=info["underlying_address"],
            decimals=info["decimals"],
        )

    def get_supported_assets(self) -> list[str]:
        """Return list of supported underlying asset symbols."""
        return list(self._token_to_vault_map.keys())

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
        #
        # EVC self-call rule: when a BatchItem's targetContract is the EVC itself
        # (the enableCollateral / enableController items), onBehalfOfAccount MUST be
        # address(0) — the account is already encoded in the call's first argument.
        # A non-zero onBehalfOfAccount on a self-call reverts with EVC_InvalidAddress()
        # (0x8133abd1). Only the borrow item targets the vault, so it carries the wallet
        # as onBehalfOfAccount, telling the EVC to set the execution context to the
        # wallet for the vault call. Verified against EthereumVaultConnector.sol
        # (callWithAuthenticationInternal) — see euler-xyz/ethereum-vault-connector.
        batch_calldata = _encode_evc_batch(
            [
                (self.evc_address, ZERO_ADDRESS, 0, bytes.fromhex(enable_collateral_data[2:])),
                (self.evc_address, ZERO_ADDRESS, 0, bytes.fromhex(enable_controller_data[2:])),
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
