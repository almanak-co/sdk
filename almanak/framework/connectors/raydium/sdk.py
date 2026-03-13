"""Raydium CLMM SDK — instruction building and pool queries.

Builds Solana instructions for Raydium CLMM LP operations using the
`solders` library. Instructions are serialized into VersionedTransactions
for execution by the SolanaExecutionPlanner.

Key operations:
- openPositionV2: Open a concentrated liquidity position with a new NFT
- increaseLiquidityV2: Add liquidity to an existing position
- decreaseLiquidityV2: Remove liquidity from a position
- closePosition: Close a position (burn NFT, recover rent)

Reference: https://github.com/raydium-io/raydium-clmm
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

from .constants import (
    ASSOCIATED_TOKEN_PROGRAM_ID,
    CLMM_PROGRAM_ID,
    CLOSE_POSITION_DISCRIMINATOR,
    DECREASE_LIQUIDITY_V2_DISCRIMINATOR,
    METADATA_PROGRAM_ID,
    OPEN_POSITION_V2_DISCRIMINATOR,
    POSITION_SEED,
    RAYDIUM_API_BASE_URL,
    RENT_SYSVAR_ID,
    SYSTEM_PROGRAM_ID,
    TICK_ARRAY_SEED,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
)

WSOL_MINT = WRAPPED_NATIVE["solana"]
from .exceptions import RaydiumAPIError, RaydiumConfigError, RaydiumPoolError
from .math import (
    align_tick_to_spacing,
    get_liquidity_from_amounts,
    price_to_tick,
    tick_array_start_index,
    tick_to_sqrt_price_x64,
)
from .models import RaydiumPool, RaydiumPosition

logger = logging.getLogger(__name__)


class RaydiumCLMMSDK:
    """SDK for building Raydium CLMM instructions.

    Provides methods to:
    - Fetch pool data from the Raydium API
    - Build Solana instructions for LP operations
    - Compute PDAs for positions, tick arrays, etc.

    Example:
        sdk = RaydiumCLMMSDK(wallet_address="your-pubkey", rpc_url="...")
        pool = sdk.get_pool_info("pool-address")
        ixs, nft_mint = sdk.build_open_position_ix(
            pool=pool,
            tick_lower=-100,
            tick_upper=100,
            amount_a=1_000_000,
            amount_b=500_000_000,
            liquidity=1000000,
        )
    """

    def __init__(
        self,
        wallet_address: str,
        base_url: str = RAYDIUM_API_BASE_URL,
        timeout: int = 30,
    ) -> None:
        if not wallet_address:
            raise RaydiumConfigError("wallet_address is required", parameter="wallet_address")

        self.wallet_address = wallet_address
        self.base_url = base_url
        self.timeout = timeout
        self._owner = Pubkey.from_string(wallet_address)
        self._program_id = Pubkey.from_string(CLMM_PROGRAM_ID)
        self._setup_session()

        logger.info(f"RaydiumCLMMSDK initialized for wallet={wallet_address[:8]}...")

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
        """Make a GET request to the Raydium API."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and not data.get("success", True):
                raise RaydiumAPIError(f"API returned error: {data}", endpoint=endpoint)
            return data
        except requests.exceptions.HTTPError as e:
            raise RaydiumAPIError(
                f"API request failed: {e}",
                status_code=getattr(e.response, "status_code", 0),
                endpoint=endpoint,
            ) from e
        except requests.exceptions.RequestException as e:
            raise RaydiumAPIError(f"Request failed: {e}", endpoint=endpoint) from e

    # =========================================================================
    # Pool queries
    # =========================================================================

    def get_pool_info(self, pool_address: str) -> RaydiumPool:
        """Fetch pool information from the Raydium API.

        Uses two endpoints:
        - /pools/key/ids: provides vault addresses, observation IDs, lookup tables
        - /pools/info/ids: provides price, TVL, volume data

        Args:
            pool_address: Pool state account address (Base58).

        Returns:
            RaydiumPool with current pool data.

        Raises:
            RaydiumPoolError: If pool not found.
            RaydiumAPIError: If API request fails.
        """
        # Fetch vault/key data (has vault addresses, observation IDs)
        key_response = self._make_request("/pools/key/ids", params={"ids": pool_address})
        key_data = key_response.get("data", []) if isinstance(key_response, dict) else key_response

        if not key_data or (isinstance(key_data, list) and key_data[0] is None):
            raise RaydiumPoolError(f"Pool not found: {pool_address}")

        pool_data = key_data[0] if isinstance(key_data, list) else key_data
        if pool_data is None:
            raise RaydiumPoolError(f"Pool not found: {pool_address}")

        # Fetch info data (has price, TVL) and merge
        try:
            info_response = self._make_request("/pools/info/ids", params={"ids": pool_address})
            info_data = info_response.get("data", []) if isinstance(info_response, dict) else info_response
            if info_data and isinstance(info_data, list) and info_data[0] is not None:
                info_item = info_data[0]
                # Merge price/tvl into pool_data (key data takes precedence for shared keys)
                for key in ("price", "tvl", "mintAmountA", "mintAmountB", "feeRate"):
                    if key in info_item and key not in pool_data:
                        pool_data[key] = info_item[key]
        except Exception:
            logger.debug(f"Could not fetch pool info data for {pool_address}, using key data only")

        pool = RaydiumPool.from_api_response(pool_data)

        if not pool.vault_a or not pool.vault_b:
            raise RaydiumPoolError(f"Pool {pool_address} missing vault addresses — cannot build transactions")

        return pool

    def find_pool_by_tokens(
        self,
        token_a: str,
        token_b: str,
        tick_spacing: int = 60,
    ) -> RaydiumPool | None:
        """Find a CLMM pool by token pair.

        Args:
            token_a: Token A mint address or symbol.
            token_b: Token B mint address or symbol.
            tick_spacing: Preferred tick spacing (default: 60 = 0.30% fee).

        Returns:
            Best matching RaydiumPool, or None if not found.
        """
        response = self._make_request(
            "/pools/info/mint",
            params={
                "mint1": token_a,
                "mint2": token_b,
                "poolType": "concentrated",
                "poolSortField": "liquidity",
                "sortType": "desc",
                "pageSize": 10,
                "page": 1,
            },
        )

        data = response.get("data", {}).get("data", []) if isinstance(response, dict) else []
        if not data:
            return None

        # Prefer pool with matching tick spacing
        for pool_data in data:
            pool = RaydiumPool.from_api_response(pool_data)
            if pool.tick_spacing == tick_spacing:
                return pool

        # Fall back to highest liquidity pool
        return RaydiumPool.from_api_response(data[0])

    # =========================================================================
    # PDA computation
    # =========================================================================

    def _find_position_pda(self, nft_mint: Pubkey) -> Pubkey:
        """Derive the PersonalPositionState PDA from an NFT mint.

        seeds: [POSITION_SEED, nft_mint_pubkey]
        """
        pda, _bump = Pubkey.find_program_address(
            [POSITION_SEED, bytes(nft_mint)],
            self._program_id,
        )
        return pda

    def _find_protocol_position_pda(self, pool: Pubkey, tick_lower: int, tick_upper: int) -> Pubkey:
        """Derive the ProtocolPositionState PDA from pool and tick range.

        seeds: [POSITION_SEED, pool_pubkey, tick_lower (i32 BE), tick_upper (i32 BE)]
        """
        pda, _bump = Pubkey.find_program_address(
            [
                POSITION_SEED,
                bytes(pool),
                struct.pack(">i", tick_lower),
                struct.pack(">i", tick_upper),
            ],
            self._program_id,
        )
        return pda

    def _find_tick_array_pda(self, pool: Pubkey, start_index: int) -> Pubkey:
        """Derive a tick array PDA.

        seeds: [TICK_ARRAY_SEED, pool_pubkey, start_index (i32 BE)]
        Note: Raydium CLMM uses big-endian for tick array start index in PDA seeds.
        """
        pda, _bump = Pubkey.find_program_address(
            [TICK_ARRAY_SEED, bytes(pool), struct.pack(">i", start_index)],
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

    # =========================================================================
    # On-chain position queries
    # =========================================================================

    def get_position_state(self, nft_mint: str, rpc_url: str) -> RaydiumPosition:
        """Query on-chain PersonalPositionState for a Raydium CLMM position.

        Derives the PDA from the NFT mint, calls getAccountInfo, and parses
        the account data to extract tick range and liquidity.

        PersonalPositionState layout (Anchor, 8-byte discriminator):
            [0:8]   discriminator
            [8:9]   bump (u8)
            [9:41]  nft_mint (Pubkey)
            [41:73] pool_id (Pubkey)
            [73:77] tick_lower_index (i32 LE)
            [77:81] tick_upper_index (i32 LE)
            [81:97] liquidity (u128 LE)

        Args:
            nft_mint: Position NFT mint address (Base58).
            rpc_url: Solana RPC endpoint URL.

        Returns:
            RaydiumPosition with on-chain tick range and liquidity.

        Raises:
            RaydiumPoolError: If position account not found or data invalid.
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
            raise RaydiumPoolError(
                f"Position account not found for NFT mint {nft_mint}. PDA: {pda}. The position may have been closed."
            )

        import base64 as b64

        account_data_b64 = result["value"]["data"][0]
        data = b64.b64decode(account_data_b64)

        if len(data) < 97:
            raise RaydiumPoolError(f"Position account data too short ({len(data)} bytes, need >= 97)")

        # Parse PersonalPositionState fields
        tick_lower = struct.unpack_from("<i", data, 73)[0]
        tick_upper = struct.unpack_from("<i", data, 77)[0]
        liquidity = int.from_bytes(data[81:97], byteorder="little")

        # Extract pool_id for cross-validation
        pool_id = Pubkey.from_bytes(data[41:73])

        logger.info(
            f"Fetched position state: nft_mint={nft_mint[:8]}..., "
            f"ticks=[{tick_lower}, {tick_upper}], liquidity={liquidity}"
        )

        return RaydiumPosition(
            nft_mint=nft_mint,
            pool_address=str(pool_id),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
            personal_position_address=str(pda),
        )

    # =========================================================================
    # Instruction builders
    # =========================================================================

    def build_open_position_ix(
        self,
        pool: RaydiumPool,
        tick_lower: int,
        tick_upper: int,
        amount_a_max: int,
        amount_b_max: int,
        liquidity: int,
        with_metadata: bool = True,
    ) -> tuple[list[Instruction], Keypair]:
        """Build instructions for opening a new CLMM position.

        Creates a new position NFT, initializes the PersonalPositionState,
        and deposits the specified amounts of token A and B.

        Args:
            pool: Pool information.
            tick_lower: Lower tick boundary (must be aligned to tick spacing).
            tick_upper: Upper tick boundary (must be aligned to tick spacing).
            amount_a_max: Maximum amount of token A in smallest units.
            amount_b_max: Maximum amount of token B in smallest units.
            liquidity: Target liquidity amount (u128).
            with_metadata: Whether to create Metaplex NFT metadata.

        Returns:
            Tuple of (instructions, nft_mint_keypair).
            The nft_mint_keypair must be included as a signer.

        Raises:
            RaydiumPoolError: If pool data is incomplete.
        """
        if not pool.vault_a or not pool.vault_b:
            raise RaydiumPoolError("Pool missing vault addresses")

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

        token_program_2022 = Pubkey.from_string(TOKEN_2022_PROGRAM_ID)

        # Compute PDAs
        personal_position = self._find_position_pda(nft_mint)
        protocol_position = self._find_protocol_position_pda(pool_pubkey, tick_lower, tick_upper)
        nft_ata = self._get_ata(self._owner, nft_mint)
        user_token_a = self._get_ata(self._owner, mint_a)
        user_token_b = self._get_ata(self._owner, mint_b)
        metadata_account = self._find_metadata_pda(nft_mint)

        # Tick array PDAs
        ta_lower_start = tick_array_start_index(tick_lower, pool.tick_spacing)
        ta_upper_start = tick_array_start_index(tick_upper, pool.tick_spacing)
        tick_array_lower = self._find_tick_array_pda(pool_pubkey, ta_lower_start)
        tick_array_upper = self._find_tick_array_pda(pool_pubkey, ta_upper_start)

        # Build instruction data
        # openPositionV2 args:
        #   tick_lower_index: i32
        #   tick_upper_index: i32
        #   tick_array_lower_start_index: i32
        #   tick_array_upper_start_index: i32
        #   liquidity: u128
        #   amount_0_max: u64
        #   amount_1_max: u64
        #   with_metadata: bool
        #   base_flag: Option<bool> = None
        ix_data = OPEN_POSITION_V2_DISCRIMINATOR + struct.pack(
            "<iiiiQQQQBB",
            tick_lower,
            tick_upper,
            ta_lower_start,
            ta_upper_start,
            liquidity & 0xFFFFFFFFFFFFFFFF,  # low 64 bits of u128
            (liquidity >> 64) & 0xFFFFFFFFFFFFFFFF,  # high 64 bits of u128
            amount_a_max,
            amount_b_max,
            1 if with_metadata else 0,
            0,  # base_flag = None (0 = no option present)
        )

        # Build account list for openPositionV2
        # Order matches the Raydium TypeScript SDK (raydium-sdk-V2)
        accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # [0] payer
            AccountMeta(self._owner, is_signer=False, is_writable=False),  # [1] position_nft_owner
            AccountMeta(nft_mint, is_signer=True, is_writable=True),  # [2] position_nft_mint
            AccountMeta(nft_ata, is_signer=False, is_writable=True),  # [3] position_nft_account
            AccountMeta(metadata_account, is_signer=False, is_writable=True),  # [4] metadata_account
            AccountMeta(pool_pubkey, is_signer=False, is_writable=True),  # [5] pool_state
            AccountMeta(protocol_position, is_signer=False, is_writable=True),  # [6] protocol_position
            AccountMeta(tick_array_lower, is_signer=False, is_writable=True),  # [7] tick_array_lower
            AccountMeta(tick_array_upper, is_signer=False, is_writable=True),  # [8] tick_array_upper
            AccountMeta(personal_position, is_signer=False, is_writable=True),  # [9] personal_position
            AccountMeta(user_token_a, is_signer=False, is_writable=True),  # [10] token_account_0
            AccountMeta(user_token_b, is_signer=False, is_writable=True),  # [11] token_account_1
            AccountMeta(vault_a, is_signer=False, is_writable=True),  # [12] token_vault_0
            AccountMeta(vault_b, is_signer=False, is_writable=True),  # [13] token_vault_1
            AccountMeta(rent, is_signer=False, is_writable=False),  # [14] rent
            AccountMeta(system_program, is_signer=False, is_writable=False),  # [15] system_program
            AccountMeta(token_program, is_signer=False, is_writable=False),  # [16] token_program
            AccountMeta(ata_program, is_signer=False, is_writable=False),  # [17] associated_token_program
            AccountMeta(metadata_program, is_signer=False, is_writable=False),  # [18] metadata_program
            AccountMeta(token_program_2022, is_signer=False, is_writable=False),  # [19] token_program_2022
            AccountMeta(mint_a, is_signer=False, is_writable=False),  # [20] vault_0_mint
            AccountMeta(mint_b, is_signer=False, is_writable=False),  # [21] vault_1_mint
        ]

        ix = Instruction(self._program_id, ix_data, accounts)

        logger.info(
            f"Built openPositionV2 ix: pool={pool.address[:8]}..., "
            f"ticks=[{tick_lower}, {tick_upper}], liquidity={liquidity}"
        )

        return [ix], nft_mint_kp

    def build_decrease_liquidity_ix(
        self,
        pool: RaydiumPool,
        position: RaydiumPosition,
        liquidity: int,
        amount_a_min: int = 0,
        amount_b_min: int = 0,
    ) -> list[Instruction]:
        """Build instructions for removing liquidity from a position.

        Args:
            pool: Pool information.
            position: Position to decrease.
            liquidity: Amount of liquidity to remove.
            amount_a_min: Minimum acceptable token A out (slippage protection).
            amount_b_min: Minimum acceptable token B out (slippage protection).

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

        personal_position = self._find_position_pda(nft_mint)
        nft_ata = self._get_ata(self._owner, nft_mint)
        user_token_a = self._get_ata(self._owner, mint_a)
        user_token_b = self._get_ata(self._owner, mint_b)

        ta_lower_start = tick_array_start_index(position.tick_lower, pool.tick_spacing)
        ta_upper_start = tick_array_start_index(position.tick_upper, pool.tick_spacing)
        tick_array_lower = self._find_tick_array_pda(pool_pubkey, ta_lower_start)
        tick_array_upper = self._find_tick_array_pda(pool_pubkey, ta_upper_start)

        # decreaseLiquidityV2 args:
        #   liquidity: u128
        #   amount_0_min: u64
        #   amount_1_min: u64
        ix_data = DECREASE_LIQUIDITY_V2_DISCRIMINATOR + struct.pack(
            "<QQQQ",
            liquidity & 0xFFFFFFFFFFFFFFFF,
            (liquidity >> 64) & 0xFFFFFFFFFFFFFFFF,
            amount_a_min,
            amount_b_min,
        )

        accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=False),  # nft_owner
            AccountMeta(nft_ata, is_signer=False, is_writable=False),  # nft_account
            AccountMeta(personal_position, is_signer=False, is_writable=True),  # personal_position
            AccountMeta(pool_pubkey, is_signer=False, is_writable=True),  # pool_state
            AccountMeta(personal_position, is_signer=False, is_writable=False),  # protocol_position (deprecated)
            AccountMeta(vault_a, is_signer=False, is_writable=True),  # token_vault_0
            AccountMeta(vault_b, is_signer=False, is_writable=True),  # token_vault_1
            AccountMeta(tick_array_lower, is_signer=False, is_writable=True),  # tick_array_lower
            AccountMeta(tick_array_upper, is_signer=False, is_writable=True),  # tick_array_upper
            AccountMeta(user_token_a, is_signer=False, is_writable=True),  # recipient_token_0
            AccountMeta(user_token_b, is_signer=False, is_writable=True),  # recipient_token_1
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
            # Token program for mint_a
            AccountMeta(token_program, is_signer=False, is_writable=False),
            # Token program for mint_b
            AccountMeta(token_program, is_signer=False, is_writable=False),
            # Memo program (required by V2)
            AccountMeta(
                Pubkey.from_string("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"), is_signer=False, is_writable=False
            ),
        ]

        ix = Instruction(self._program_id, ix_data, accounts)

        logger.info(f"Built decreaseLiquidityV2 ix: pool={pool.address[:8]}..., liquidity={liquidity}")

        return [ix]

    def build_close_position_ix(
        self,
        position: RaydiumPosition,
    ) -> list[Instruction]:
        """Build instructions for closing a position (burn NFT, recover rent).

        The position must have zero liquidity and zero fees owed.

        Args:
            position: Position to close.

        Returns:
            List of Solana instructions.
        """
        nft_mint = Pubkey.from_string(position.nft_mint)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)

        personal_position = self._find_position_pda(nft_mint)
        nft_ata = self._get_ata(self._owner, nft_mint)

        ix_data = CLOSE_POSITION_DISCRIMINATOR

        accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # nft_owner
            AccountMeta(nft_mint, is_signer=False, is_writable=True),  # position_nft_mint
            AccountMeta(nft_ata, is_signer=False, is_writable=True),  # position_nft_account
            AccountMeta(personal_position, is_signer=False, is_writable=True),  # personal_position
            AccountMeta(system_program, is_signer=False, is_writable=False),  # system_program
            AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program
        ]

        ix = Instruction(self._program_id, ix_data, accounts)

        logger.info(f"Built closePosition ix: nft_mint={position.nft_mint[:8]}...")

        return [ix]

    # =========================================================================
    # ATA setup helpers
    # =========================================================================

    def _build_ata_setup_instructions(self, pool: RaydiumPool, amount_a_lamports: int) -> list[Instruction]:
        """Build instructions to create ATAs and wrap SOL if token A is WSOL.

        For Raydium LP, the token accounts must exist before the openPositionV2
        instruction. If token A is Wrapped SOL, we also need to transfer native
        SOL and sync_native to make it available as WSOL.

        Args:
            pool: Pool information (used to determine if token A is WSOL).
            amount_a_lamports: Amount of token A in lamports (used for WSOL wrapping).

        Returns:
            List of setup instructions (may be empty if ATAs already exist).
        """
        from solders.system_program import TransferParams, transfer

        ixs: list[Instruction] = []
        mint_a = Pubkey.from_string(pool.mint_a)
        mint_b = Pubkey.from_string(pool.mint_b)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_program = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)

        is_token_a_wsol = pool.mint_a == WSOL_MINT

        # Create ATA for token A (idempotent — createIdempotent won't fail if exists)
        user_ata_a = self._get_ata(self._owner, mint_a)
        ixs.append(
            Instruction(
                ata_program,
                bytes([1]),  # CreateIdempotent instruction discriminator
                [
                    AccountMeta(self._owner, is_signer=True, is_writable=True),  # funding
                    AccountMeta(user_ata_a, is_signer=False, is_writable=True),  # ata
                    AccountMeta(self._owner, is_signer=False, is_writable=False),  # wallet
                    AccountMeta(mint_a, is_signer=False, is_writable=False),  # mint
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
            # Transfer native SOL to the WSOL ATA
            ixs.append(
                transfer(
                    TransferParams(
                        from_pubkey=self._owner,
                        to_pubkey=user_ata_a,
                        lamports=amount_a_lamports,
                    )
                )
            )
            # SyncNative: update WSOL balance to reflect native SOL transfer
            # SPL Token SyncNative instruction = discriminator byte 17
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
        pool: RaydiumPool,
        price_lower: float,
        price_upper: float,
        amount_a: int,
        amount_b: int,
        slippage_bps: int = 100,
    ) -> tuple[list[Instruction], Keypair, dict[str, Any]]:
        """Build a complete open position transaction from price bounds.

        High-level method that handles tick conversion, alignment,
        liquidity calculation, and slippage.

        Args:
            pool: Pool information.
            price_lower: Lower price bound (human-readable, token_b per token_a).
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
            raise RaydiumPoolError(
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
        pool: RaydiumPool,
        position: RaydiumPosition,
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
                amount_a_min=0,  # Accept any amount (slippage handled by amounts)
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
