"""Orca Whirlpools SDK — instruction building and pool queries.

Builds Solana instructions for Orca Whirlpool LP operations using the
`solders` library. Instructions are serialized into VersionedTransactions
for execution by the SolanaExecutionPlanner.

Key operations:
- open_position_with_metadata: Open a position with a new NFT
- increase_liquidity: Add tokens to an existing position
- decrease_liquidity: Remove tokens from a position
- close_position: Close a position (burn NFT, recover rent)

Reuses Raydium CLMM math module for Q64.64 tick calculations since
Orca Whirlpools uses the identical concentrated liquidity model.

Reference: https://github.com/orca-so/whirlpools
"""

from __future__ import annotations

import logging
import struct
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from urllib3.util.retry import Retry

from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

from ..raydium.math import (
    align_tick_to_spacing,
    get_liquidity_from_amounts,
    price_to_tick,
    tick_to_sqrt_price_x64,
)
from .constants import (
    ASSOCIATED_TOKEN_PROGRAM_ID,
    CLOSE_POSITION_DISCRIMINATOR,
    DECREASE_LIQUIDITY_DISCRIMINATOR,
    INCREASE_LIQUIDITY_DISCRIMINATOR,
    METADATA_PROGRAM_ID,
    OPEN_POSITION_WITH_METADATA_DISCRIMINATOR,
    ORCA_API_BASE_URL,
    POSITION_SEED,
    RENT_SYSVAR_ID,
    SYSTEM_PROGRAM_ID,
    TICK_ARRAY_SEED,
    TICK_ARRAY_SIZE,
    TOKEN_PROGRAM_ID,
    WHIRLPOOL_PROGRAM_ID,
)

WSOL_MINT = WRAPPED_NATIVE["solana"]
from .exceptions import OrcaAPIError, OrcaConfigError, OrcaPoolError
from .models import OrcaPool, OrcaPosition

logger = logging.getLogger(__name__)


class OrcaWhirlpoolSDK:
    """SDK for building Orca Whirlpool instructions.

    Provides methods to:
    - Fetch pool data from the Orca API
    - Build Solana instructions for LP operations
    - Compute PDAs for positions, tick arrays, etc.

    Example:
        sdk = OrcaWhirlpoolSDK(wallet_address="your-pubkey")
        pool = sdk.get_pool_info("pool-address")
        ixs, nft_mint = sdk.build_open_position_ix(
            pool=pool,
            tick_lower=-100,
            tick_upper=100,
            amount_a_max=1_000_000,
            amount_b_max=500_000_000,
            liquidity=1000000,
        )
    """

    def __init__(
        self,
        wallet_address: str,
        base_url: str = ORCA_API_BASE_URL,
        timeout: int = 30,
    ) -> None:
        if not wallet_address:
            raise OrcaConfigError("wallet_address is required", parameter="wallet_address")

        self.wallet_address = wallet_address
        self.base_url = base_url
        self.timeout = timeout
        self._owner = Pubkey.from_string(wallet_address)
        self._program_id = Pubkey.from_string(WHIRLPOOL_PROGRAM_ID)
        self._setup_session()

        logger.info(f"OrcaWhirlpoolSDK initialized for wallet={wallet_address[:8]}...")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Make a GET request to the Orca API."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise OrcaAPIError(
                f"API request failed: {e}",
                status_code=getattr(e.response, "status_code", 0),
                endpoint=endpoint,
            ) from e
        except requests.exceptions.RequestException as e:
            raise OrcaAPIError(f"Request failed: {e}", endpoint=endpoint) from e

    # =========================================================================
    # Pool queries
    # =========================================================================

    def get_pool_info(self, pool_address: str) -> OrcaPool:
        """Fetch pool information from the Orca API.

        Args:
            pool_address: Whirlpool account address (Base58).

        Returns:
            OrcaPool with current pool data.

        Raises:
            OrcaPoolError: If pool not found.
            OrcaAPIError: If API request fails.
        """
        data = self._make_request(f"/pools/{pool_address}")

        if not data or (isinstance(data, dict) and not data.get("address")):
            raise OrcaPoolError(f"Pool not found: {pool_address}")

        pool = OrcaPool.from_api_response(data)

        if not pool.vault_a or not pool.vault_b:
            raise OrcaPoolError(f"Pool {pool_address} missing vault addresses — cannot build transactions")

        return pool

    def find_pool_by_tokens(
        self,
        token_a: str,
        token_b: str,
        tick_spacing: int = 64,
    ) -> OrcaPool | None:
        """Find a Whirlpool by token pair.

        Args:
            token_a: Token A mint address.
            token_b: Token B mint address.
            tick_spacing: Preferred tick spacing (default: 64 = 0.30% fee).

        Returns:
            Best matching OrcaPool, or None if not found.
        """
        response = self._make_request(
            "/pools/search",
            params={"q": f"{token_a},{token_b}"},
        )

        pools = response if isinstance(response, list) else response.get("data", [])
        if not pools:
            return None

        # Prefer pool with matching tick spacing
        for pool_data in pools:
            pool = OrcaPool.from_api_response(pool_data)
            if pool.tick_spacing == tick_spacing:
                return pool

        # Fall back to first pool (highest liquidity typically)
        return OrcaPool.from_api_response(pools[0])

    # =========================================================================
    # PDA computation
    # =========================================================================

    def _find_position_pda(self, nft_mint: Pubkey) -> Pubkey:
        """Derive the Position PDA from an NFT mint.

        seeds: [POSITION_SEED, nft_mint_pubkey]
        """
        pda, _bump = Pubkey.find_program_address(
            [POSITION_SEED, bytes(nft_mint)],
            self._program_id,
        )
        return pda

    def _find_tick_array_pda(self, pool: Pubkey, start_index: int) -> Pubkey:
        """Derive a tick array PDA.

        seeds: [TICK_ARRAY_SEED, pool_pubkey, start_index (string)]
        Note: Orca uses the string representation of the start index as seed.
        """
        pda, _bump = Pubkey.find_program_address(
            [TICK_ARRAY_SEED, bytes(pool), str(start_index).encode()],
            self._program_id,
        )
        return pda

    def _find_oracle_pda(self, pool: Pubkey) -> Pubkey:
        """Derive the Oracle PDA for a pool.

        seeds: [b"oracle", pool_pubkey]
        """
        pda, _bump = Pubkey.find_program_address(
            [b"oracle", bytes(pool)],
            self._program_id,
        )
        return pda

    def _find_metadata_pda(self, nft_mint: Pubkey) -> Pubkey:
        """Derive the Metaplex metadata PDA for a mint."""
        metadata_program = Pubkey.from_string(METADATA_PROGRAM_ID)
        pda, _bump = Pubkey.find_program_address(
            [b"metadata", bytes(metadata_program), bytes(nft_mint)],
            metadata_program,
        )
        return pda

    def _get_ata(self, owner: Pubkey, mint: Pubkey, token_program: Pubkey | None = None) -> Pubkey:
        """Compute the Associated Token Account address."""
        token_prog = token_program or Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_prog = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        pda, _bump = Pubkey.find_program_address(
            [bytes(owner), bytes(token_prog), bytes(mint)],
            ata_prog,
        )
        return pda

    @staticmethod
    def _tick_array_start_index(tick: int, tick_spacing: int) -> int:
        """Compute the start index of the tick array containing the given tick.

        Orca uses TICK_ARRAY_SIZE (88) ticks per array.
        """
        array_size = TICK_ARRAY_SIZE * tick_spacing
        if tick >= 0:
            return (tick // array_size) * array_size
        else:
            return -(((-tick - 1) // array_size + 1) * array_size)

    # =========================================================================
    # On-chain position queries
    # =========================================================================

    def get_position_state(self, nft_mint: str, rpc_url: str) -> OrcaPosition:
        """Query on-chain Position account for an Orca Whirlpool position.

        Position layout (Anchor, 8-byte discriminator):
            [0:8]    discriminator
            [8:40]   whirlpool (Pubkey)
            [40:72]  position_mint (Pubkey)
            [72:88]  liquidity (u128 LE)
            [88:92]  tick_lower_index (i32 LE)
            [92:96]  tick_upper_index (i32 LE)

        Args:
            nft_mint: Position NFT mint address (Base58).
            rpc_url: Solana RPC endpoint URL.

        Returns:
            OrcaPosition with on-chain tick range and liquidity.

        Raises:
            OrcaPoolError: If position account not found or data invalid.
        """
        nft_mint_pubkey = Pubkey.from_string(nft_mint)
        pda = self._find_position_pda(nft_mint_pubkey)

        resp = self.session.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [str(pda), {"encoding": "base64"}],
            },
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json().get("result")

        if not result or not result.get("value"):
            raise OrcaPoolError(
                f"Position account not found for NFT mint {nft_mint}. PDA: {pda}. The position may have been closed."
            )

        import base64 as b64

        account_data_b64 = result["value"]["data"][0]
        data = b64.b64decode(account_data_b64)

        if len(data) < 96:
            raise OrcaPoolError(f"Position account data too short ({len(data)} bytes, need >= 96)")

        # Parse Position fields
        pool_id = Pubkey.from_bytes(data[8:40])
        liquidity = int.from_bytes(data[72:88], byteorder="little")
        tick_lower = struct.unpack_from("<i", data, 88)[0]
        tick_upper = struct.unpack_from("<i", data, 92)[0]

        logger.info(
            f"Fetched Orca position state: nft_mint={nft_mint[:8]}..., "
            f"ticks=[{tick_lower}, {tick_upper}], liquidity={liquidity}"
        )

        return OrcaPosition(
            nft_mint=nft_mint,
            pool_address=str(pool_id),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
            position_address=str(pda),
        )

    # =========================================================================
    # Instruction builders
    # =========================================================================

    def build_open_position_ix(
        self,
        pool: OrcaPool,
        tick_lower: int,
        tick_upper: int,
        amount_a_max: int,
        amount_b_max: int,
        liquidity: int,
    ) -> tuple[list[Instruction], Keypair]:
        """Build instructions for opening a new Whirlpool position.

        Creates: open_position_with_metadata + increase_liquidity.
        Orca separates position creation (open_position) from liquidity
        deposit (increase_liquidity).

        Args:
            pool: Pool information.
            tick_lower: Lower tick boundary (aligned to tick spacing).
            tick_upper: Upper tick boundary (aligned to tick spacing).
            amount_a_max: Maximum amount of token A in smallest units.
            amount_b_max: Maximum amount of token B in smallest units.
            liquidity: Target liquidity amount (u128).

        Returns:
            Tuple of (instructions, nft_mint_keypair).
        """
        if not pool.vault_a or not pool.vault_b:
            raise OrcaPoolError("Pool missing vault addresses")

        # Generate a new keypair for the position NFT mint
        nft_mint_kp = Keypair()
        nft_mint = nft_mint_kp.pubkey()

        pool_pubkey = Pubkey.from_string(pool.address)
        mint_a = Pubkey.from_string(pool.mint_a)
        mint_b = Pubkey.from_string(pool.mint_b)
        vault_a = Pubkey.from_string(pool.vault_a)
        vault_b = Pubkey.from_string(pool.vault_b)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)
        ata_program = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        rent = Pubkey.from_string(RENT_SYSVAR_ID)
        metadata_program = Pubkey.from_string(METADATA_PROGRAM_ID)

        # Compute PDAs
        position_pda = self._find_position_pda(nft_mint)
        nft_ata = self._get_ata(self._owner, nft_mint)
        metadata_account = self._find_metadata_pda(nft_mint)
        user_token_a = self._get_ata(self._owner, mint_a)
        user_token_b = self._get_ata(self._owner, mint_b)

        # Tick array PDAs
        ta_lower_start = self._tick_array_start_index(tick_lower, pool.tick_spacing)
        ta_upper_start = self._tick_array_start_index(tick_upper, pool.tick_spacing)
        tick_array_lower = self._find_tick_array_pda(pool_pubkey, ta_lower_start)
        tick_array_upper = self._find_tick_array_pda(pool_pubkey, ta_upper_start)

        ixs: list[Instruction] = []

        # --- Instruction 1: open_position_with_metadata ---
        # Args: bumps.position_bump (u8), bumps.metadata_bump (u8),
        #       tick_lower_index (i32), tick_upper_index (i32)
        # We pass 0 for bumps — the program derives them
        open_ix_data = OPEN_POSITION_WITH_METADATA_DISCRIMINATOR + struct.pack(
            "<BBii",
            0,  # position_bump (program derives)
            0,  # metadata_bump (program derives)
            tick_lower,
            tick_upper,
        )

        open_accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # funder
            AccountMeta(self._owner, is_signer=False, is_writable=False),  # owner
            AccountMeta(position_pda, is_signer=False, is_writable=True),  # position
            AccountMeta(nft_mint, is_signer=True, is_writable=True),  # position_mint
            AccountMeta(nft_ata, is_signer=False, is_writable=True),  # position_token_account
            AccountMeta(pool_pubkey, is_signer=False, is_writable=False),  # whirlpool
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
            AccountMeta(system_program, is_signer=False, is_writable=False),  # system_program
            AccountMeta(rent, is_signer=False, is_writable=False),  # rent
            AccountMeta(ata_program, is_signer=False, is_writable=False),  # associated_token_program
            AccountMeta(metadata_program, is_signer=False, is_writable=False),  # metadata_program
            AccountMeta(metadata_account, is_signer=False, is_writable=True),  # metadata_account
            AccountMeta(
                Pubkey.from_string("METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m"),
                is_signer=False,
                is_writable=False,
            ),  # metadata_update_auth
        ]

        ixs.append(Instruction(self._program_id, open_ix_data, open_accounts))

        # --- Instruction 2: increase_liquidity ---
        # Args: liquidity_amount (u128), token_max_a (u64), token_max_b (u64)
        increase_ix_data = INCREASE_LIQUIDITY_DISCRIMINATOR + struct.pack(
            "<QQQQ",
            liquidity & 0xFFFFFFFFFFFFFFFF,  # low 64 bits of u128
            (liquidity >> 64) & 0xFFFFFFFFFFFFFFFF,  # high 64 bits of u128
            amount_a_max,
            amount_b_max,
        )

        increase_accounts = [
            AccountMeta(pool_pubkey, is_signer=False, is_writable=True),  # whirlpool
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
            AccountMeta(self._owner, is_signer=True, is_writable=False),  # position_authority
            AccountMeta(position_pda, is_signer=False, is_writable=True),  # position
            AccountMeta(nft_ata, is_signer=False, is_writable=False),  # position_token_account
            AccountMeta(user_token_a, is_signer=False, is_writable=True),  # token_owner_account_a
            AccountMeta(user_token_b, is_signer=False, is_writable=True),  # token_owner_account_b
            AccountMeta(vault_a, is_signer=False, is_writable=True),  # token_vault_a
            AccountMeta(vault_b, is_signer=False, is_writable=True),  # token_vault_b
            AccountMeta(tick_array_lower, is_signer=False, is_writable=True),  # tick_array_lower
            AccountMeta(tick_array_upper, is_signer=False, is_writable=True),  # tick_array_upper
        ]

        ixs.append(Instruction(self._program_id, increase_ix_data, increase_accounts))

        logger.info(
            f"Built Orca open_position + increase_liquidity: pool={pool.address[:8]}..., "
            f"ticks=[{tick_lower}, {tick_upper}], liquidity={liquidity}"
        )

        return ixs, nft_mint_kp

    def build_decrease_liquidity_ix(
        self,
        pool: OrcaPool,
        position: OrcaPosition,
        liquidity: int,
        amount_a_min: int = 0,
        amount_b_min: int = 0,
    ) -> list[Instruction]:
        """Build instructions for removing liquidity from a position.

        Args:
            pool: Pool information.
            position: Position to decrease.
            liquidity: Amount of liquidity to remove.
            amount_a_min: Minimum acceptable token A out.
            amount_b_min: Minimum acceptable token B out.

        Returns:
            List of Solana instructions.
        """
        pool_pubkey = Pubkey.from_string(pool.address)
        nft_mint = Pubkey.from_string(position.nft_mint)
        mint_a = Pubkey.from_string(pool.mint_a)
        mint_b = Pubkey.from_string(pool.mint_b)
        vault_a = Pubkey.from_string(pool.vault_a)
        vault_b = Pubkey.from_string(pool.vault_b)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)

        position_pda = self._find_position_pda(nft_mint)
        nft_ata = self._get_ata(self._owner, nft_mint)
        user_token_a = self._get_ata(self._owner, mint_a)
        user_token_b = self._get_ata(self._owner, mint_b)

        ta_lower_start = self._tick_array_start_index(position.tick_lower, pool.tick_spacing)
        ta_upper_start = self._tick_array_start_index(position.tick_upper, pool.tick_spacing)
        tick_array_lower = self._find_tick_array_pda(pool_pubkey, ta_lower_start)
        tick_array_upper = self._find_tick_array_pda(pool_pubkey, ta_upper_start)

        # decrease_liquidity args:
        #   liquidity_amount: u128
        #   token_min_a: u64
        #   token_min_b: u64
        ix_data = DECREASE_LIQUIDITY_DISCRIMINATOR + struct.pack(
            "<QQQQ",
            liquidity & 0xFFFFFFFFFFFFFFFF,
            (liquidity >> 64) & 0xFFFFFFFFFFFFFFFF,
            amount_a_min,
            amount_b_min,
        )

        accounts = [
            AccountMeta(pool_pubkey, is_signer=False, is_writable=True),  # whirlpool
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
            AccountMeta(self._owner, is_signer=True, is_writable=False),  # position_authority
            AccountMeta(position_pda, is_signer=False, is_writable=True),  # position
            AccountMeta(nft_ata, is_signer=False, is_writable=False),  # position_token_account
            AccountMeta(user_token_a, is_signer=False, is_writable=True),  # token_owner_account_a
            AccountMeta(user_token_b, is_signer=False, is_writable=True),  # token_owner_account_b
            AccountMeta(vault_a, is_signer=False, is_writable=True),  # token_vault_a
            AccountMeta(vault_b, is_signer=False, is_writable=True),  # token_vault_b
            AccountMeta(tick_array_lower, is_signer=False, is_writable=True),  # tick_array_lower
            AccountMeta(tick_array_upper, is_signer=False, is_writable=True),  # tick_array_upper
        ]

        ix = Instruction(self._program_id, ix_data, accounts)
        logger.info(f"Built Orca decrease_liquidity ix: pool={pool.address[:8]}..., liquidity={liquidity}")
        return [ix]

    def build_close_position_ix(
        self,
        position: OrcaPosition,
    ) -> list[Instruction]:
        """Build instructions for closing a position (burn NFT, recover rent).

        Args:
            position: Position to close.

        Returns:
            List of Solana instructions.
        """
        nft_mint = Pubkey.from_string(position.nft_mint)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)

        position_pda = self._find_position_pda(nft_mint)
        nft_ata = self._get_ata(self._owner, nft_mint)

        ix_data = CLOSE_POSITION_DISCRIMINATOR

        accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # position_authority
            AccountMeta(self._owner, is_signer=False, is_writable=True),  # receiver
            AccountMeta(position_pda, is_signer=False, is_writable=True),  # position
            AccountMeta(nft_mint, is_signer=False, is_writable=True),  # position_mint
            AccountMeta(nft_ata, is_signer=False, is_writable=True),  # position_token_account
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
        ]

        ix = Instruction(self._program_id, ix_data, accounts)
        logger.info(f"Built Orca close_position ix: nft_mint={position.nft_mint[:8]}...")
        return [ix]

    # =========================================================================
    # ATA setup helpers
    # =========================================================================

    def _build_ata_setup_instructions(self, pool: OrcaPool, amount_a_lamports: int) -> list[Instruction]:
        """Build instructions to create ATAs and wrap SOL if token A is WSOL."""
        from solders.system_program import TransferParams, transfer

        ixs: list[Instruction] = []
        mint_a = Pubkey.from_string(pool.mint_a)
        mint_b = Pubkey.from_string(pool.mint_b)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_program = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)

        is_token_a_wsol = pool.mint_a == WSOL_MINT

        # Create ATA for token A (idempotent)
        user_ata_a = self._get_ata(self._owner, mint_a)
        ixs.append(
            Instruction(
                ata_program,
                bytes([1]),  # CreateIdempotent
                [
                    AccountMeta(self._owner, is_signer=True, is_writable=True),
                    AccountMeta(user_ata_a, is_signer=False, is_writable=True),
                    AccountMeta(self._owner, is_signer=False, is_writable=False),
                    AccountMeta(mint_a, is_signer=False, is_writable=False),
                    AccountMeta(system_program, is_signer=False, is_writable=False),
                    AccountMeta(token_program, is_signer=False, is_writable=False),
                ],
            )
        )

        # Create ATA for token B (idempotent)
        user_ata_b = self._get_ata(self._owner, mint_b)
        ixs.append(
            Instruction(
                ata_program,
                bytes([1]),  # CreateIdempotent
                [
                    AccountMeta(self._owner, is_signer=True, is_writable=True),
                    AccountMeta(user_ata_b, is_signer=False, is_writable=True),
                    AccountMeta(self._owner, is_signer=False, is_writable=False),
                    AccountMeta(mint_b, is_signer=False, is_writable=False),
                    AccountMeta(system_program, is_signer=False, is_writable=False),
                    AccountMeta(token_program, is_signer=False, is_writable=False),
                ],
            )
        )

        # If token A is WSOL, transfer native SOL and sync_native
        if is_token_a_wsol and amount_a_lamports > 0:
            ixs.append(
                transfer(
                    TransferParams(
                        from_pubkey=self._owner,
                        to_pubkey=user_ata_a,
                        lamports=amount_a_lamports,
                    )
                )
            )
            ixs.append(
                Instruction(
                    token_program,
                    bytes([17]),  # SyncNative
                    [AccountMeta(user_ata_a, is_signer=False, is_writable=True)],
                )
            )

        return ixs

    # =========================================================================
    # High-level transaction builders
    # =========================================================================

    def build_open_position_transaction(
        self,
        pool: OrcaPool,
        price_lower: float,
        price_upper: float,
        amount_a: int,
        amount_b: int,
        slippage_bps: int = 100,
    ) -> tuple[list[Instruction], Keypair, dict[str, Any]]:
        """Build a complete open position transaction from price bounds.

        Args:
            pool: Pool information.
            price_lower: Lower price bound (token_b per token_a).
            price_upper: Upper price bound.
            amount_a: Amount of token A in smallest units.
            amount_b: Amount of token B in smallest units.
            slippage_bps: Slippage tolerance in basis points (default: 100 = 1%).

        Returns:
            Tuple of (instructions, nft_mint_keypair, metadata).
        """
        from decimal import Decimal

        # Convert prices to ticks and align
        tick_lower_raw = price_to_tick(Decimal(str(price_lower)), pool.decimals_a, pool.decimals_b)
        tick_upper_raw = price_to_tick(Decimal(str(price_upper)), pool.decimals_a, pool.decimals_b)

        tick_lower = align_tick_to_spacing(tick_lower_raw, pool.tick_spacing, round_up=False)
        tick_upper = align_tick_to_spacing(tick_upper_raw, pool.tick_spacing, round_up=True)

        # Calculate sqrt prices
        sqrt_price_current = tick_to_sqrt_price_x64(
            price_to_tick(Decimal(str(pool.current_price)), pool.decimals_a, pool.decimals_b)
        )
        sqrt_price_lower = tick_to_sqrt_price_x64(tick_lower)
        sqrt_price_upper = tick_to_sqrt_price_x64(tick_upper)

        # Calculate liquidity from amounts
        liquidity = get_liquidity_from_amounts(
            sqrt_price_current, sqrt_price_lower, sqrt_price_upper, amount_a, amount_b
        )

        if liquidity <= 0:
            raise OrcaPoolError(
                f"Calculated liquidity is zero for amounts ({amount_a}, {amount_b}) "
                f"in tick range [{tick_lower}, {tick_upper}]"
            )

        # Apply slippage to max amounts
        slippage_multiplier = 1 + (slippage_bps / 10000)
        amount_a_max = int(amount_a * slippage_multiplier)
        amount_b_max = int(amount_b * slippage_multiplier)

        # Build pre-instructions: create ATAs + wrap SOL if needed
        pre_ixs = self._build_ata_setup_instructions(pool, amount_a_max)

        ixs, nft_mint_kp = self.build_open_position_ix(
            pool=pool,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            amount_a_max=amount_a_max,
            amount_b_max=amount_b_max,
            liquidity=liquidity,
        )

        # Prepend ATA setup instructions
        ixs = pre_ixs + ixs

        metadata = {
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "liquidity": str(liquidity),
            "amount_a_max": amount_a_max,
            "amount_b_max": amount_b_max,
            "nft_mint": str(nft_mint_kp.pubkey()),
            "slippage_bps": slippage_bps,
        }

        return ixs, nft_mint_kp, metadata

    def build_close_position_transaction(
        self,
        pool: OrcaPool,
        position: OrcaPosition,
        slippage_bps: int = 100,
    ) -> tuple[list[Instruction], dict[str, Any]]:
        """Build instructions to fully close a position.

        Decreases all liquidity, then closes the position account.

        Args:
            pool: Pool information.
            position: Position to close.
            slippage_bps: Slippage tolerance for decrease liquidity.

        Returns:
            Tuple of (all_instructions, metadata).
        """
        all_ixs: list[Instruction] = []

        # Step 1: Decrease all liquidity if any remains
        if position.liquidity > 0:
            decrease_ixs = self.build_decrease_liquidity_ix(
                pool=pool,
                position=position,
                liquidity=position.liquidity,
                amount_a_min=0,
                amount_b_min=0,
            )
            all_ixs.extend(decrease_ixs)

        # Step 2: Close position (burn NFT, recover rent)
        close_ixs = self.build_close_position_ix(position)
        all_ixs.extend(close_ixs)

        metadata = {
            "nft_mint": position.nft_mint,
            "pool": pool.address,
            "liquidity_removed": str(position.liquidity),
            "slippage_bps": slippage_bps,
        }

        return all_ixs, metadata
