"""Drift Protocol SDK — PDA derivation, instruction building, account parsing.

Builds Solana instructions for Drift perpetual futures using the `solders`
library. Instructions are serialized into VersionedTransactions for execution
by the SolanaExecutionPlanner.

Key operations:
- place_perp_order: Open/close perpetual positions (market or limit orders)
- initialize_user: Create a new Drift user account
- initialize_user_stats: Create user stats account
- deposit: Deposit collateral (USDC) into Drift

Pattern: Raw instruction building (same as Raydium CLMM connector),
NOT REST API. This avoids the driftpy dependency conflict (solders <0.27.0).

Reference: https://github.com/drift-labs/protocol-v2
"""

from __future__ import annotations

import logging
import struct

import requests
from requests.adapters import HTTPAdapter
from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey
from urllib3.util.retry import Retry

from .constants import (
    ASSOCIATED_TOKEN_PROGRAM_ID,
    DEPOSIT_DISCRIMINATOR,
    DRIFT_PROGRAM_ID,
    INITIALIZE_USER_DISCRIMINATOR,
    INITIALIZE_USER_STATS_DISCRIMINATOR,
    MAX_PERP_POSITIONS,
    MAX_SPOT_POSITIONS,
    PERP_MARKET_ORACLE_OFFSET,
    PERP_MARKET_SEED,
    PERP_POSITION_SIZE,
    PLACE_PERP_ORDER_DISCRIMINATOR,
    SPOT_MARKET_ORACLE_OFFSET,
    SPOT_MARKET_SEED,
    SPOT_POSITION_SIZE,
    STATE_SEED,
    SYSTEM_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    USDC_MINT,
    USER_AUTHORITY_OFFSET,
    USER_PERP_POSITIONS_OFFSET,
    USER_SEED,
    USER_SPOT_POSITIONS_OFFSET,
    USER_STATS_SEED,
)
from .exceptions import DriftAccountNotFoundError, DriftConfigError, DriftMarketError
from .models import (
    DriftPerpPosition,
    DriftSpotPosition,
    DriftUserAccount,
    OrderParams,
)

logger = logging.getLogger(__name__)


class DriftSDK:
    """SDK for building Drift protocol instructions.

    Provides methods to:
    - Derive PDAs for Drift accounts (state, user, markets)
    - Build Solana instructions for perp trading
    - Fetch and parse on-chain account data via RPC
    - Build remaining accounts lists for order placement

    Example:
        sdk = DriftSDK(wallet_address="your-pubkey", rpc_url="https://...")
        ix = sdk.build_place_perp_order_ix(
            order_params=OrderParams(direction=DIRECTION_LONG, ...),
            remaining_accounts=[...],
        )
    """

    def __init__(
        self,
        wallet_address: str,
        rpc_url: str = "",
        timeout: int = 30,
    ) -> None:
        if not wallet_address:
            raise DriftConfigError("wallet_address is required", parameter="wallet_address")

        self.wallet_address = wallet_address
        self.rpc_url = rpc_url
        self.timeout = timeout
        self._authority = Pubkey.from_string(wallet_address)
        self._program_id = Pubkey.from_string(DRIFT_PROGRAM_ID)
        self._setup_session()

        logger.info(f"DriftSDK initialized for wallet={wallet_address[:8]}...")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic for RPC calls."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # =========================================================================
    # PDA Derivation (pure functions, no RPC needed)
    # =========================================================================

    def get_state_pda(self) -> Pubkey:
        """Derive the Drift state account PDA."""
        pda, _bump = Pubkey.find_program_address(
            [STATE_SEED],
            self._program_id,
        )
        return pda

    def get_user_pda(self, authority: Pubkey | None = None, sub_account_id: int = 0) -> Pubkey:
        """Derive a Drift User account PDA.

        Args:
            authority: Wallet pubkey (defaults to SDK's wallet)
            sub_account_id: Sub-account ID (default 0)
        """
        auth = authority or self._authority
        pda, _bump = Pubkey.find_program_address(
            [
                USER_SEED,
                bytes(auth),
                struct.pack("<H", sub_account_id),
            ],
            self._program_id,
        )
        return pda

    def get_user_stats_pda(self, authority: Pubkey | None = None) -> Pubkey:
        """Derive the User Stats account PDA."""
        auth = authority or self._authority
        pda, _bump = Pubkey.find_program_address(
            [USER_STATS_SEED, bytes(auth)],
            self._program_id,
        )
        return pda

    def get_perp_market_pda(self, market_index: int) -> Pubkey:
        """Derive a Perp Market account PDA."""
        pda, _bump = Pubkey.find_program_address(
            [PERP_MARKET_SEED, struct.pack("<H", market_index)],
            self._program_id,
        )
        return pda

    def get_spot_market_pda(self, market_index: int) -> Pubkey:
        """Derive a Spot Market account PDA."""
        pda, _bump = Pubkey.find_program_address(
            [SPOT_MARKET_SEED, struct.pack("<H", market_index)],
            self._program_id,
        )
        return pda

    # =========================================================================
    # On-chain Account Fetching & Parsing
    # =========================================================================

    def fetch_user_account(self, sub_account_id: int = 0) -> DriftUserAccount:
        """Fetch and parse a Drift User account from on-chain data.

        Args:
            sub_account_id: Sub-account ID to fetch

        Returns:
            DriftUserAccount with parsed positions, or exists=False if not found
        """
        if not self.rpc_url:
            return DriftUserAccount(exists=False)

        user_pda = self.get_user_pda(sub_account_id=sub_account_id)
        account_data = self._fetch_account_data(str(user_pda))

        if account_data is None:
            return DriftUserAccount(
                authority=self.wallet_address,
                sub_account_id=sub_account_id,
                exists=False,
            )

        return self._parse_user_account(account_data, sub_account_id)

    def fetch_market_oracle(self, market_index: int) -> Pubkey | None:
        """Fetch the oracle pubkey from a perp market account.

        Args:
            market_index: Perp market index

        Returns:
            Oracle Pubkey or None if not found
        """
        if not self.rpc_url:
            return None

        market_pda = self.get_perp_market_pda(market_index)
        account_data = self._fetch_account_data(str(market_pda))

        if account_data is None:
            raise DriftMarketError(
                f"Perp market {market_index} account not found on-chain",
                market=str(market_index),
            )

        # Oracle pubkey is at a fixed offset in the PerpMarket account
        if len(account_data) < PERP_MARKET_ORACLE_OFFSET + 32:
            raise DriftMarketError(
                f"Perp market {market_index} account data too short",
                market=str(market_index),
            )

        oracle_bytes = account_data[PERP_MARKET_ORACLE_OFFSET : PERP_MARKET_ORACLE_OFFSET + 32]
        return Pubkey.from_bytes(oracle_bytes)

    def fetch_spot_market_oracle(self, market_index: int) -> Pubkey | None:
        """Fetch the oracle pubkey from a spot market account."""
        if not self.rpc_url:
            return None

        market_pda = self.get_spot_market_pda(market_index)
        account_data = self._fetch_account_data(str(market_pda))

        if account_data is None:
            return None

        if len(account_data) < SPOT_MARKET_ORACLE_OFFSET + 32:
            return None

        oracle_bytes = account_data[SPOT_MARKET_ORACLE_OFFSET : SPOT_MARKET_ORACLE_OFFSET + 32]
        return Pubkey.from_bytes(oracle_bytes)

    def _fetch_account_data(self, address: str) -> bytes | None:
        """Fetch raw account data via RPC getAccountInfo.

        Args:
            address: Base58 account address

        Returns:
            Raw account data bytes, or None if account doesn't exist
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                address,
                {"encoding": "base64", "commitment": "confirmed"},
            ],
        }

        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()

            value = result.get("result", {}).get("value")
            if value is None:
                return None

            import base64

            data_b64 = value.get("data", [None])[0]
            if data_b64 is None:
                return None

            return base64.b64decode(data_b64)

        except requests.exceptions.RequestException as e:
            logger.warning(f"RPC request failed for {address}: {e}")
            return None

    def _parse_user_account(self, data: bytes, sub_account_id: int) -> DriftUserAccount:
        """Parse raw bytes into a DriftUserAccount.

        Extracts active perp and spot positions from the User account layout.
        Validates authority field to detect stale layout offsets.
        """
        # Sanity check: verify the authority field matches our wallet address.
        # If the Drift program upgrades and changes the account layout, this
        # will catch offset drift before we read garbage position data.
        if len(data) >= USER_AUTHORITY_OFFSET + 32:
            authority_bytes = data[USER_AUTHORITY_OFFSET : USER_AUTHORITY_OFFSET + 32]
            try:
                on_chain_authority = str(Pubkey.from_bytes(authority_bytes))
                if on_chain_authority != self.wallet_address:
                    logger.warning(
                        "User account authority mismatch: expected %s, got %s. Account layout offsets may be stale.",
                        self.wallet_address,
                        on_chain_authority,
                    )
            except Exception:
                logger.warning("Failed to parse authority from user account data")

        perp_positions: list[DriftPerpPosition] = []
        spot_positions: list[DriftSpotPosition] = []

        # Parse perp positions
        for i in range(MAX_PERP_POSITIONS):
            offset = USER_PERP_POSITIONS_OFFSET + (i * PERP_POSITION_SIZE)
            if offset + PERP_POSITION_SIZE > len(data):
                break
            pos = self._parse_perp_position(data, offset)
            perp_positions.append(pos)

        # Parse spot positions
        for i in range(MAX_SPOT_POSITIONS):
            offset = USER_SPOT_POSITIONS_OFFSET + (i * SPOT_POSITION_SIZE)
            if offset + SPOT_POSITION_SIZE > len(data):
                break
            spot_pos = self._parse_spot_position(data, offset)
            spot_positions.append(spot_pos)

        return DriftUserAccount(
            authority=self.wallet_address,
            sub_account_id=sub_account_id,
            perp_positions=perp_positions,
            spot_positions=spot_positions,
            exists=True,
        )

    def _parse_perp_position(self, data: bytes, offset: int) -> DriftPerpPosition:
        """Parse a single perp position from account data.

        Layout (80 bytes):
        - base_asset_amount: i64 (offset+0)
        - quote_asset_amount: i64 (offset+8)
        - last_cumulative_funding_rate: i64 (offset+16)
        - market_index: u16 (offset+24)
        - ... (remaining fields)
        - open_orders: u8 (offset+72)
        """
        base_asset_amount = struct.unpack_from("<q", data, offset)[0]
        quote_asset_amount = struct.unpack_from("<q", data, offset + 8)[0]
        last_cumulative_funding_rate = struct.unpack_from("<q", data, offset + 16)[0]
        market_index = struct.unpack_from("<H", data, offset + 24)[0]
        open_orders = data[offset + 72] if offset + 72 < len(data) else 0

        return DriftPerpPosition(
            market_index=market_index,
            base_asset_amount=base_asset_amount,
            quote_asset_amount=quote_asset_amount,
            last_cumulative_funding_rate=last_cumulative_funding_rate,
            open_orders=open_orders,
        )

    def _parse_spot_position(self, data: bytes, offset: int) -> DriftSpotPosition:
        """Parse a single spot position from account data.

        Layout (48 bytes):
        - scaled_balance: u64 (offset+0)
        - market_index: u16 (offset+8)
        - balance_type: u8 (offset+10)
        - open_orders: u8 (offset+11)
        """
        scaled_balance = struct.unpack_from("<Q", data, offset)[0]
        market_index = struct.unpack_from("<H", data, offset + 8)[0]
        balance_type = data[offset + 10] if offset + 10 < len(data) else 0
        open_orders = data[offset + 11] if offset + 11 < len(data) else 0

        return DriftSpotPosition(
            market_index=market_index,
            scaled_balance=scaled_balance,
            balance_type=balance_type,
            open_orders=open_orders,
        )

    # =========================================================================
    # Remaining Accounts Builder
    # =========================================================================

    def build_remaining_accounts(
        self,
        market_index: int,
        sub_account_id: int = 0,
    ) -> list[AccountMeta]:
        """Build the remaining accounts list for a place_perp_order instruction.

        Drift requires remaining accounts to include all markets the user has
        positions in, plus their oracles. The order is:
        1. Oracle accounts (readable)
        2. Spot market accounts (readable)
        3. Perp market accounts (readable)

        Args:
            market_index: The perp market being traded
            sub_account_id: Sub-account ID

        Returns:
            List of AccountMeta for remaining accounts
        """
        oracle_accounts: list[Pubkey] = []
        spot_market_accounts: list[Pubkey] = []
        perp_market_accounts: list[Pubkey] = []

        # Track which markets we need to include
        perp_indexes: set[int] = {market_index}
        spot_indexes: set[int] = {0}  # Always include USDC (spot market 0)

        # If we have RPC, fetch user account to find existing positions
        if self.rpc_url:
            user_account = self.fetch_user_account(sub_account_id)
            if user_account.exists:
                perp_indexes.update(user_account.active_perp_market_indexes)
                spot_indexes.update(user_account.active_spot_market_indexes)

        # Fetch oracle addresses for each market
        for idx in sorted(perp_indexes):
            perp_market_pda = self.get_perp_market_pda(idx)
            perp_market_accounts.append(perp_market_pda)

            oracle = self._get_oracle_for_perp_market(idx)
            if oracle:
                oracle_accounts.append(oracle)

        for idx in sorted(spot_indexes):
            spot_market_pda = self.get_spot_market_pda(idx)
            spot_market_accounts.append(spot_market_pda)

            oracle = self._get_oracle_for_spot_market(idx)
            if oracle:
                oracle_accounts.append(oracle)

        # Deduplicate oracles (a single oracle can serve multiple markets)
        seen_oracles: set[str] = set()
        unique_oracles: list[Pubkey] = []
        for oracle in oracle_accounts:
            oracle_str = str(oracle)
            if oracle_str not in seen_oracles:
                seen_oracles.add(oracle_str)
                unique_oracles.append(oracle)

        # Build remaining accounts in the required order
        remaining: list[AccountMeta] = []

        # 1. Oracles (not signer, not writable)
        for oracle in unique_oracles:
            remaining.append(AccountMeta(oracle, is_signer=False, is_writable=False))

        # 2. Spot markets (not signer, not writable)
        for spot_market in spot_market_accounts:
            remaining.append(AccountMeta(spot_market, is_signer=False, is_writable=False))

        # 3. Perp markets (not signer, writable for the traded market)
        for perp_market in perp_market_accounts:
            remaining.append(AccountMeta(perp_market, is_signer=False, is_writable=True))

        return remaining

    def _get_oracle_for_perp_market(self, market_index: int) -> Pubkey | None:
        """Get oracle address for a perp market, fetching from chain if needed."""
        try:
            return self.fetch_market_oracle(market_index)
        except (DriftMarketError, DriftAccountNotFoundError):
            logger.warning(f"Could not fetch oracle for perp market {market_index}")
            return None

    def _get_oracle_for_spot_market(self, market_index: int) -> Pubkey | None:
        """Get oracle address for a spot market."""
        try:
            return self.fetch_spot_market_oracle(market_index)
        except (DriftMarketError, DriftAccountNotFoundError):
            logger.warning(f"Could not fetch oracle for spot market {market_index}")
            return None

    # =========================================================================
    # Instruction Builders
    # =========================================================================

    def build_place_perp_order_ix(
        self,
        order_params: OrderParams,
        remaining_accounts: list[AccountMeta],
        sub_account_id: int = 0,
    ) -> Instruction:
        """Build a place_perp_order instruction.

        Args:
            order_params: Order parameters to encode
            remaining_accounts: Pre-built remaining accounts (oracles, markets)
            sub_account_id: Sub-account ID

        Returns:
            Solana Instruction ready for transaction
        """
        state_pda = self.get_state_pda()
        user_pda = self.get_user_pda(sub_account_id=sub_account_id)

        ix_data = PLACE_PERP_ORDER_DISCRIMINATOR + self._encode_order_params(order_params)

        accounts = [
            AccountMeta(state_pda, is_signer=False, is_writable=False),
            AccountMeta(user_pda, is_signer=False, is_writable=True),
            AccountMeta(self._authority, is_signer=True, is_writable=False),
        ]
        accounts.extend(remaining_accounts)

        return Instruction(self._program_id, ix_data, accounts)

    def build_initialize_user_ix(self, sub_account_id: int = 0) -> Instruction:
        """Build an initialize_user instruction.

        Creates a new Drift User account for the wallet.
        Must be called before the first order if no account exists.
        """
        state_pda = self.get_state_pda()
        user_pda = self.get_user_pda(sub_account_id=sub_account_id)
        user_stats_pda = self.get_user_stats_pda()

        # Encode: sub_account_id (u16) + name ([u8; 32], zeroed)
        ix_data = INITIALIZE_USER_DISCRIMINATOR + struct.pack("<H", sub_account_id) + bytes(32)

        accounts = [
            AccountMeta(user_pda, is_signer=False, is_writable=True),
            AccountMeta(user_stats_pda, is_signer=False, is_writable=True),
            AccountMeta(state_pda, is_signer=False, is_writable=False),
            AccountMeta(self._authority, is_signer=True, is_writable=True),
            AccountMeta(Pubkey.from_string(SYSTEM_PROGRAM_ID), is_signer=False, is_writable=False),
        ]

        return Instruction(self._program_id, ix_data, accounts)

    def build_initialize_user_stats_ix(self) -> Instruction:
        """Build an initialize_user_stats instruction.

        Must be called before initialize_user if no user stats exist.
        """
        state_pda = self.get_state_pda()
        user_stats_pda = self.get_user_stats_pda()

        ix_data = INITIALIZE_USER_STATS_DISCRIMINATOR

        accounts = [
            AccountMeta(user_stats_pda, is_signer=False, is_writable=True),
            AccountMeta(state_pda, is_signer=False, is_writable=False),
            AccountMeta(self._authority, is_signer=True, is_writable=True),
            AccountMeta(Pubkey.from_string(SYSTEM_PROGRAM_ID), is_signer=False, is_writable=False),
        ]

        return Instruction(self._program_id, ix_data, accounts)

    def build_deposit_ix(
        self,
        amount: int,
        market_index: int = 0,
        sub_account_id: int = 0,
    ) -> Instruction:
        """Build a deposit instruction.

        Deposits collateral (typically USDC) into a Drift spot market.

        Args:
            amount: Amount in smallest units (e.g., USDC with 6 decimals)
            market_index: Spot market index (0 = USDC)
            sub_account_id: Sub-account ID
        """
        state_pda = self.get_state_pda()
        user_pda = self.get_user_pda(sub_account_id=sub_account_id)
        user_stats_pda = self.get_user_stats_pda()
        spot_market_pda = self.get_spot_market_pda(market_index)

        # User's token account (ATA for the deposit token)
        token_mint = Pubkey.from_string(USDC_MINT) if market_index == 0 else self.get_spot_market_pda(market_index)
        user_token_account = self._get_ata(self._authority, token_mint)

        # Spot market vault — derived from spot market PDA
        # For simplicity we'll use a placeholder; the real vault address
        # should be read from the spot market account data
        spot_market_vault = self._get_spot_market_vault(market_index)

        # Encode: market_index (u16) + amount (u64) + reduce_only (bool as u8)
        ix_data = DEPOSIT_DISCRIMINATOR + struct.pack("<HQB", market_index, amount, 0)

        accounts = [
            AccountMeta(state_pda, is_signer=False, is_writable=False),
            AccountMeta(user_pda, is_signer=False, is_writable=True),
            AccountMeta(user_stats_pda, is_signer=False, is_writable=True),
            AccountMeta(self._authority, is_signer=True, is_writable=False),
            AccountMeta(spot_market_pda, is_signer=False, is_writable=True),
            AccountMeta(spot_market_vault, is_signer=False, is_writable=True),
            AccountMeta(user_token_account, is_signer=False, is_writable=True),
            AccountMeta(Pubkey.from_string(TOKEN_PROGRAM_ID), is_signer=False, is_writable=False),
        ]

        return Instruction(self._program_id, ix_data, accounts)

    def _get_spot_market_vault(self, market_index: int) -> Pubkey:
        """Derive the spot market vault PDA."""
        pda, _bump = Pubkey.find_program_address(
            [b"spot_market_vault", struct.pack("<H", market_index)],
            self._program_id,
        )
        return pda

    # =========================================================================
    # User Account Initialization Check
    # =========================================================================

    def get_init_instructions(self, sub_account_id: int = 0) -> list[Instruction]:
        """Get instructions to initialize user accounts if they don't exist.

        Returns an empty list if accounts already exist.
        """
        instructions: list[Instruction] = []

        if not self.rpc_url:
            # Without RPC we can't check, assume accounts exist
            return instructions

        # Check if user stats account exists
        user_stats_pda = self.get_user_stats_pda()
        stats_data = self._fetch_account_data(str(user_stats_pda))
        if stats_data is None:
            instructions.append(self.build_initialize_user_stats_ix())

        # Check if user account exists
        user_account = self.fetch_user_account(sub_account_id)
        if not user_account.exists:
            instructions.append(self.build_initialize_user_ix(sub_account_id))

        if instructions:
            logger.info(f"Drift user accounts need initialization ({len(instructions)} instructions)")

        return instructions

    # =========================================================================
    # Borsh Encoding
    # =========================================================================

    def _encode_order_params(self, params: OrderParams) -> bytes:
        """Borsh-encode OrderParams struct for Drift's place_perp_order.

        Layout:
        - order_type: u8
        - market_type: u8
        - direction: u8
        - user_order_id: u8
        - base_asset_amount: u64
        - price: u64
        - market_index: u16
        - reduce_only: bool (u8)
        - post_only: u8
        - bit_flags: u8 (bit 0=IOC, bit 1=HighLeverageMode)
        - max_ts: Option<i64>    (1 byte tag + 8 bytes if Some)
        - trigger_price: Option<u64>
        - trigger_condition: u8
        - oracle_price_offset: Option<i32>
        - auction_duration: Option<u8>
        - auction_start_price: Option<i64>
        - auction_end_price: Option<i64>
        """
        buf = bytearray()

        # Fixed fields
        buf.extend(
            struct.pack(
                "<BBBBQQHBBB",
                params.order_type,
                params.market_type,
                params.direction,
                params.user_order_id,
                params.base_asset_amount,
                params.price,
                params.market_index,
                1 if params.reduce_only else 0,
                params.post_only,
                params.bit_flags,
            )
        )

        # Option<i64> max_ts
        buf.extend(self._encode_option_i64(params.max_ts))

        # Option<u64> trigger_price
        buf.extend(self._encode_option_u64(params.trigger_price))

        # trigger_condition: u8
        buf.extend(struct.pack("<B", params.trigger_condition))

        # Option<i32> oracle_price_offset
        buf.extend(self._encode_option_i32(params.oracle_price_offset))

        # Option<u8> auction_duration
        buf.extend(self._encode_option_u8(params.auction_duration))

        # Option<i64> auction_start_price
        buf.extend(self._encode_option_i64(params.auction_start_price))

        # Option<i64> auction_end_price
        buf.extend(self._encode_option_i64(params.auction_end_price))

        return bytes(buf)

    @staticmethod
    def _encode_option_i64(value: int | None) -> bytes:
        """Encode Option<i64> in Borsh format."""
        if value is None:
            return b"\x00"
        return b"\x01" + struct.pack("<q", value)

    @staticmethod
    def _encode_option_u64(value: int | None) -> bytes:
        """Encode Option<u64> in Borsh format."""
        if value is None:
            return b"\x00"
        return b"\x01" + struct.pack("<Q", value)

    @staticmethod
    def _encode_option_i32(value: int | None) -> bytes:
        """Encode Option<i32> in Borsh format."""
        if value is None:
            return b"\x00"
        return b"\x01" + struct.pack("<i", value)

    @staticmethod
    def _encode_option_u8(value: int | None) -> bytes:
        """Encode Option<u8> in Borsh format."""
        if value is None:
            return b"\x00"
        return b"\x01" + struct.pack("<B", value)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _get_ata(self, owner: Pubkey, mint: Pubkey) -> Pubkey:
        """Compute the Associated Token Account address."""
        token_prog = Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_prog = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        pda, _bump = Pubkey.find_program_address(
            [bytes(owner), bytes(token_prog), bytes(mint)],
            ata_prog,
        )
        return pda
