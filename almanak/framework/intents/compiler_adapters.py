"""Compiler protocol adapters — DEX and lending adapters.

These are extracted from compiler.py for file-size management.
All symbols remain importable from ``almanak.framework.intents.compiler``.
"""

from typing import Protocol

from .compiler_constants import (
    AAVE_BORROW_SELECTOR,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_V2_DEPOSIT_SELECTOR,
    AAVE_V2_FORKS,
    AAVE_WITHDRAW_SELECTOR,
    BALANCER_FLASH_LOAN_SELECTOR,
    BALANCER_VAULT_ADDRESSES,
    LENDING_POOL_ADDRESSES,
    get_gas_estimate,
)

# =============================================================================
# Protocol Adapter Protocol
# =============================================================================


class SwapProtocolAdapter(Protocol):
    """Protocol interface for DEX adapters."""

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for a swap transaction."""
        ...

    def get_router_address(self) -> str:
        """Get the router address for this protocol."""
        ...

    def estimate_gas(self, from_token: str, to_token: str) -> int:
        """Estimate gas for a swap."""
        ...


class LendingProtocolAdapter(Protocol):
    """Protocol interface for lending adapters."""

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying collateral."""
        ...

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing tokens."""
        ...

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens."""
        ...

    def get_pool_address(self) -> str:
        """Get the lending pool address for this protocol."""
        ...

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        ...

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        ...

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        ...


# =============================================================================
# Connector-owned swap adapters
# =============================================================================

from almanak.connectors._strategy_base.base.swap_adapter import (  # noqa: E402,F401
    _BRIDGED_USDC_PROBE_CHAINS,
    _CHAIN_WRAPPED_NATIVE,
    DefaultSwapAdapter,
)


class AaveV3Adapter:
    """Lending adapter for Aave V3 protocol.

    This adapter generates calldata for interacting with Aave V3 lending pools,
    supporting supply, borrow, and repay operations.

    Aave V3 features:
    - Efficiency Mode (E-Mode) for higher LTVs between correlated assets
    - Isolation Mode for new assets with limited debt ceiling
    - Variable and stable interest rates (stable being deprecated)
    """

    _AAVE_V2_FORKS = AAVE_V2_FORKS

    def __init__(self, chain: str, protocol: str = "aave_v3") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for pool lookup
        """
        self.chain = chain
        self.protocol = protocol
        self._is_v2_fork = protocol in self._AAVE_V2_FORKS

        # Get pool address
        chain_pools = LENDING_POOL_ADDRESSES.get(chain, {})
        self.pool_address = chain_pools.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_pool_address(self) -> str:
        """Get the Aave V3 Pool address."""
        return self.pool_address

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying assets.

        Aave V3: supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
        Aave V2 forks (Radiant V2): deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)

        Both have identical parameter layouts, only the function selector differs.

        Args:
            asset: Token address to supply
            amount: Amount to supply (in token's smallest units)
            on_behalf_of: Address to credit with the supply

        Returns:
            Encoded calldata for the supply/deposit transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_address(on_behalf_of)
            + self._pad_uint16(referral_code)
        )

        selector = AAVE_V2_DEPOSIT_SELECTOR if self._is_v2_fork else AAVE_SUPPLY_SELECTOR
        return bytes.fromhex(selector[2:] + params)

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing from Aave V3.

        Aave V3 borrow function:
        borrow(address asset, uint256 amount, uint256 interestRateMode,
               uint16 referralCode, address onBehalfOf)

        Args:
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address to debit with the borrow

        Returns:
            Encoded calldata for the borrow transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_uint16(referral_code)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_BORROW_SELECTOR[2:] + params)

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens to Aave V3.

        Aave V3 repay function:
        repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)

        To repay the full debt, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to repay
            amount: Amount to repay (in token's smallest units), MAX_UINT256 for full
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address that has the debt being repaid

        Returns:
            Encoded calldata for the repay transaction
        """
        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_REPAY_SELECTOR[2:] + params)

    def get_withdraw_calldata(
        self,
        asset: str,
        amount: int,
        to: str,
    ) -> bytes:
        """Generate calldata for withdrawing supplied assets from Aave V3.

        Aave V3 withdraw function:
        withdraw(address asset, uint256 amount, address to)

        To withdraw all supplied assets, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to withdraw
            amount: Amount to withdraw (in token's smallest units), MAX_UINT256 for full
            to: Address to receive the withdrawn tokens

        Returns:
            Encoded calldata for the withdraw transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(amount) + self._pad_address(to)

        return bytes.fromhex(AAVE_WITHDRAW_SELECTOR[2:] + params)

    def get_set_collateral_calldata(
        self,
        asset: str,
        use_as_collateral: bool,
    ) -> bytes:
        """Generate calldata for enabling/disabling an asset as collateral.

        Aave V3 setUserUseReserveAsCollateral function:
        setUserUseReserveAsCollateral(address asset, bool useAsCollateral)

        This must be called after supplying to enable borrowing against the asset.

        Args:
            asset: Token address to enable/disable as collateral
            use_as_collateral: True to enable, False to disable

        Returns:
            Encoded calldata for the setUserUseReserveAsCollateral transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(1 if use_as_collateral else 0)

        return bytes.fromhex(AAVE_SET_COLLATERAL_SELECTOR[2:] + params)

    def estimate_set_collateral_gas(self) -> int:
        """Estimate gas for setUserUseReserveAsCollateral operation."""
        return 150000  # Aave V3 can use more gas with incentives

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        return get_gas_estimate(self.chain, "lending_supply")

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        return get_gas_estimate(self.chain, "lending_borrow")

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        return get_gas_estimate(self.chain, "lending_repay")

    def estimate_withdraw_gas(self) -> int:
        """Estimate gas for withdraw operation."""
        return get_gas_estimate(self.chain, "lending_withdraw")

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for flash loan operation (base only, not including callbacks)."""
        return get_gas_estimate(self.chain, "flash_loan")

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for simple flash loan operation (base only, not including callbacks)."""
        return get_gas_estimate(self.chain, "flash_loan_simple")

    def get_flash_loan_simple_calldata(
        self,
        receiver_address: str,
        asset: str,
        amount: int,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a simple (single-asset) flash loan.

        Aave V3 flashLoanSimple function:
        flashLoanSimple(
            address receiverAddress,
            address asset,
            uint256 amount,
            bytes calldata params,
            uint16 referralCode
        )

        The receiver contract must implement executeOperation() and return the
        borrowed amount plus premium (0.09% on Aave) within the same transaction.

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoanSimple transaction
        """
        # Calculate params offset (after fixed params: 5 * 32 bytes)
        params_offset = 5 * 32  # receiver(32) + asset(32) + amount(32) + paramsOffset(32) + referralCode(32)

        # Encode params data
        params_hex = params.hex() if params else ""
        params_len = len(params)

        encoded = (
            self._pad_address(receiver_address)
            + self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(params_offset)
            + self._pad_uint16(0)  # referral code
            + self._pad_uint256(params_len)
        )

        if params_len > 0:
            # Pad params to 32-byte boundary
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SIMPLE_SELECTOR[2:] + encoded)

    def get_flash_loan_calldata(
        self,
        receiver_address: str,
        assets: list[str],
        amounts: list[int],
        modes: list[int],
        on_behalf_of: str,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a multi-asset flash loan.

        Aave V3 flashLoan function:
        flashLoan(
            address receiverAddress,
            address[] calldata assets,
            uint256[] calldata amounts,
            uint256[] calldata modes,
            address onBehalfOf,
            bytes calldata params,
            uint16 referralCode
        )

        Modes:
        - 0: No debt opened (must repay within same transaction) - for atomic arb
        - 1: Open stable rate debt
        - 2: Open variable rate debt

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            assets: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            modes: List of debt modes (0, 1, or 2) for each asset
            on_behalf_of: Address to receive debt if mode != 0
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_assets = len(assets)

        # Calculate offsets for dynamic arrays
        # Fixed params before arrays: receiverAddress(32) + 3 array offsets(32*3) + onBehalfOf(32) + params offset(32) + referralCode(32) = 7*32
        assets_offset = 7 * 32
        amounts_offset = assets_offset + 32 + n_assets * 32  # length(32) + data(32*n)
        modes_offset = amounts_offset + 32 + n_assets * 32
        params_offset = modes_offset + 32 + n_assets * 32

        # Build header
        encoded = self._pad_address(receiver_address)
        encoded += self._pad_uint256(assets_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(modes_offset)
        encoded += self._pad_address(on_behalf_of)
        encoded += self._pad_uint256(params_offset)
        encoded += self._pad_uint16(0)  # referral code

        # Encode assets array
        encoded += self._pad_uint256(n_assets)
        for addr in assets:
            encoded += self._pad_address(addr)

        # Encode amounts array
        encoded += self._pad_uint256(n_assets)
        for amount_val in amounts:
            encoded += self._pad_uint256(amount_val)

        # Encode modes array
        encoded += self._pad_uint256(n_assets)
        for mode in modes:
            encoded += self._pad_uint256(mode)

        # Encode params
        params_hex = params.hex() if params else ""
        params_len = len(params)
        encoded += self._pad_uint256(params_len)
        if params_len > 0:
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SELECTOR[2:] + encoded)

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint16(value: int) -> str:
        """Pad uint16 to 32 bytes."""
        return hex(value)[2:].zfill(64)


class BalancerAdapter:
    """Flash loan adapter for Balancer Vault.

    Balancer flash loans have zero fees (no premium), making them ideal for
    arbitrage strategies. The Vault contract holds all pool liquidity.

    Balancer Vault flash loan function:
    flashLoan(
        IFlashLoanRecipient recipient,
        IERC20[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    )

    Key differences from Aave:
    - Zero fees (no premium to repay)
    - All tokens and amounts in arrays (batch flash loans native)
    - userData is arbitrary bytes passed to receiver
    - Receiver must implement receiveFlashLoan() not executeOperation()
    """

    def __init__(self, chain: str, protocol: str = "balancer") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name (always "balancer")
        """
        self.chain = chain
        self.protocol = protocol

        # Get vault address
        self.vault_address = BALANCER_VAULT_ADDRESSES.get(chain, "0x0000000000000000000000000000000000000000")

    def get_vault_address(self) -> str:
        """Get the Balancer Vault address."""
        return self.vault_address

    def get_flash_loan_calldata(
        self,
        recipient: str,
        tokens: list[str],
        amounts: list[int],
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a Balancer flash loan.

        Balancer flashLoan function:
        flashLoan(
            IFlashLoanRecipient recipient,
            IERC20[] memory tokens,
            uint256[] memory amounts,
            bytes memory userData
        )

        Args:
            recipient: Contract address that will receive and handle the flash loan
            tokens: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_tokens = len(tokens)
        if n_tokens != len(amounts):
            raise ValueError("tokens and amounts must have same length")

        # ABI encoding for flashLoan(address,address[],uint256[],bytes)
        # Layout:
        # - recipient (32 bytes, padded address)
        # - offset to tokens array (32 bytes)
        # - offset to amounts array (32 bytes)
        # - offset to userData (32 bytes)
        # - tokens array: length (32) + addresses (32 * n)
        # - amounts array: length (32) + amounts (32 * n)
        # - userData: length (32) + data (padded to 32)

        # Calculate offsets
        # Fixed header: recipient(32) + 3 offsets(32*3) = 128 bytes
        tokens_offset = 128
        amounts_offset = tokens_offset + 32 + n_tokens * 32
        user_data_offset = amounts_offset + 32 + n_tokens * 32

        # Build header
        encoded = self._pad_address(recipient)
        encoded += self._pad_uint256(tokens_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(user_data_offset)

        # Encode tokens array
        encoded += self._pad_uint256(n_tokens)
        for token in tokens:
            encoded += self._pad_address(token)

        # Encode amounts array
        encoded += self._pad_uint256(n_tokens)
        for amount in amounts:
            encoded += self._pad_uint256(amount)

        # Encode userData
        user_data_hex = user_data.hex() if user_data else ""
        user_data_len = len(user_data)
        encoded += self._pad_uint256(user_data_len)
        if user_data_len > 0:
            # Pad to 32-byte boundary
            padded_data = user_data_hex + "0" * ((64 - len(user_data_hex) % 64) % 64)
            encoded += padded_data

        return bytes.fromhex(BALANCER_FLASH_LOAN_SELECTOR[2:] + encoded)

    def get_flash_loan_simple_calldata(
        self,
        recipient: str,
        token: str,
        amount: int,
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a single-token flash loan.

        This is a convenience method that wraps get_flash_loan_calldata
        for single-token flash loans.

        Args:
            recipient: Contract address that will receive the flash loan
            token: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        return self.get_flash_loan_calldata(
            recipient=recipient,
            tokens=[token],
            amounts=[amount],
            user_data=user_data,
        )

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for a multi-token flash loan (base only, not including callbacks)."""
        return get_gas_estimate(self.chain, "balancer_flash_loan")

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for a single-token flash loan (base only, not including callbacks)."""
        return get_gas_estimate(self.chain, "balancer_flash_loan_simple")

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad an address to 32 bytes (64 hex chars)."""
        clean_addr = addr.lower().replace("0x", "")
        return clean_addr.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad a uint256 to 32 bytes (64 hex chars)."""
        return hex(value)[2:].zfill(64)
