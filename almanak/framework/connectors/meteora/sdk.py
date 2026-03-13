"""Meteora DLMM SDK -- instruction building and pool queries.

Builds Solana instructions for Meteora DLMM LP operations using the
`solders` library. Instructions are serialized into VersionedTransactions
for execution by the SolanaExecutionPlanner.

Key operations:
- initializePosition: Create a new position account
- addLiquidityByStrategy: Deposit tokens into bins
- removeLiquidityByRange: Withdraw tokens from a bin range
- closePosition: Close position account, recover rent

Reference: https://docs.meteora.ag/dlmm/
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
    ADD_LIQUIDITY_BY_STRATEGY_DISCRIMINATOR,
    ASSOCIATED_TOKEN_PROGRAM_ID,
    CLOSE_POSITION_DISCRIMINATOR,
    DLMM_PROGRAM_ID,
    EVENT_AUTHORITY_SEED,
    INITIALIZE_POSITION_DISCRIMINATOR,
    METEORA_API_BASE_URL,
    ORACLE_SEED,
    POSITION_SEED,
    REMOVE_LIQUIDITY_BY_RANGE_DISCRIMINATOR,
    RENT_SYSVAR_ID,
    STRATEGY_TYPE_SPOT_BALANCED,
    SYSTEM_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
)

WSOL_MINT = WRAPPED_NATIVE["solana"]
from .exceptions import MeteoraAPIError, MeteoraPoolError, MeteoraPositionError
from .math import get_bin_array_index, get_bin_array_pda
from .models import MeteoraPool, MeteoraPosition

logger = logging.getLogger(__name__)


class MeteoraSDK:
    """SDK for building Meteora DLMM instructions.

    Provides methods to:
    - Fetch pool data from the Meteora DLMM API
    - Build Solana instructions for LP operations
    - Compute PDAs for positions, bin arrays, etc.

    Example:
        sdk = MeteoraSDK(wallet_address="your-pubkey")
        pool = sdk.get_pool("pool-address")
        ixs, position_kp = sdk.build_open_position_transaction(
            pool=pool, lower_bin_id=8388600, upper_bin_id=8388620,
            amount_x=1_000_000, amount_y=500_000_000,
        )
    """

    def __init__(
        self,
        wallet_address: str,
        base_url: str = METEORA_API_BASE_URL,
        timeout: int = 30,
    ) -> None:
        if not wallet_address:
            raise ValueError("wallet_address is required")

        self.wallet_address = wallet_address
        self.base_url = base_url
        self.timeout = timeout
        self._owner = Pubkey.from_string(wallet_address)
        self._program_id = Pubkey.from_string(DLMM_PROGRAM_ID)
        self._setup_session()

        logger.info(f"MeteoraSDK initialized for wallet={wallet_address[:8]}...")

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
        """Make a GET request to the Meteora DLMM API."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise MeteoraAPIError(
                f"API request failed: {e}",
                status_code=getattr(e.response, "status_code", 0),
                endpoint=endpoint,
            ) from e
        except requests.exceptions.RequestException as e:
            raise MeteoraAPIError(f"Request failed: {e}", endpoint=endpoint) from e

    # =========================================================================
    # Pool queries
    # =========================================================================

    def get_pool(self, pool_address: str) -> MeteoraPool:
        """Fetch pool information from the Meteora DLMM API.

        Args:
            pool_address: Pool (lb_pair) account address (Base58).

        Returns:
            MeteoraPool with current pool data.

        Raises:
            MeteoraPoolError: If pool not found.
            MeteoraAPIError: If API request fails.
        """
        data = self._make_request(f"/pair/{pool_address}")

        if not data:
            raise MeteoraPoolError(f"Pool not found: {pool_address}")

        pool = MeteoraPool.from_api_response(data)

        if not pool.mint_x or not pool.mint_y:
            raise MeteoraPoolError(f"Pool {pool_address} missing mint addresses")

        return pool

    def find_pool(self, token_a: str, token_b: str) -> MeteoraPool | None:
        """Find a DLMM pool by token pair.

        Searches the Meteora API for pools matching the token pair and
        returns the one with highest TVL.

        Args:
            token_a: Token A mint address.
            token_b: Token B mint address.

        Returns:
            Best matching MeteoraPool, or None if not found.
        """
        data = self._make_request("/pair/all_with_pagination", params={"limit": 100})

        if not data:
            return None

        pairs = data if isinstance(data, list) else data.get("pairs", data.get("data", []))

        candidates = []
        for pair_data in pairs:
            mint_x = pair_data.get("mint_x", pair_data.get("mintX", ""))
            mint_y = pair_data.get("mint_y", pair_data.get("mintY", ""))

            if (mint_x == token_a and mint_y == token_b) or (mint_x == token_b and mint_y == token_a):
                candidates.append(pair_data)

        if not candidates:
            return None

        # Sort by TVL descending
        candidates.sort(key=lambda x: float(x.get("liquidity", x.get("tvl", 0))), reverse=True)
        return MeteoraPool.from_api_response(candidates[0])

    def get_active_bin(self, pool_address: str) -> dict[str, Any]:
        """Get the active bin for a pool.

        Args:
            pool_address: Pool address.

        Returns:
            Dict with bin_id, price, and amounts.
        """
        pool = self.get_pool(pool_address)
        return {
            "bin_id": pool.active_bin_id,
            "price": pool.current_price,
        }

    # =========================================================================
    # PDA computation
    # =========================================================================

    def get_position_pda(self, lb_pair: Pubkey, base: Pubkey, lower_bin_id: int, width: int) -> Pubkey:
        """Derive the position PDA.

        seeds: [POSITION_SEED, lb_pair, base, lower_bin_id (i32 LE), width (i32 LE)]
        """
        pda, _bump = Pubkey.find_program_address(
            [
                POSITION_SEED,
                bytes(lb_pair),
                bytes(base),
                struct.pack("<i", lower_bin_id),
                struct.pack("<i", width),
            ],
            self._program_id,
        )
        return pda

    def get_event_authority_pda(self) -> Pubkey:
        """Derive the event authority PDA."""
        pda, _bump = Pubkey.find_program_address(
            [EVENT_AUTHORITY_SEED],
            self._program_id,
        )
        return pda

    def get_oracle_pda(self, lb_pair: Pubkey) -> Pubkey:
        """Derive the oracle PDA for a pool."""
        pda, _bump = Pubkey.find_program_address(
            [ORACLE_SEED, bytes(lb_pair)],
            self._program_id,
        )
        return pda

    def _get_ata(self, owner: Pubkey, mint: Pubkey) -> Pubkey:
        """Compute the Associated Token Account address."""
        token_prog = Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_prog = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        pda, _bump = Pubkey.find_program_address(
            [bytes(owner), bytes(token_prog), bytes(mint)],
            ata_prog,
        )
        return pda

    # =========================================================================
    # On-chain position queries
    # =========================================================================

    def get_position_state(self, position_address: str, rpc_url: str) -> MeteoraPosition:
        """Query on-chain position state for a Meteora DLMM position.

        Meteora PositionV2 layout (Anchor, 8-byte discriminator):
            [0:8]   discriminator
            [8:40]  lb_pair (Pubkey)
            [40:72] owner (Pubkey)
            [72:76] lower_bin_id (i32 LE)
            [76:80] upper_bin_id (i32 LE)

        Args:
            position_address: Position account address (Base58).
            rpc_url: Solana RPC endpoint URL.

        Returns:
            MeteoraPosition with on-chain state.

        Raises:
            MeteoraPositionError: If position account not found or data invalid.
        """
        resp = self.session.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [position_address, {"encoding": "base64"}],
            },
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json().get("result")

        if not result or not result.get("value"):
            raise MeteoraPositionError(
                f"Position account not found: {position_address}. The position may have been closed."
            )

        import base64 as b64

        account_data_b64 = result["value"]["data"][0]
        data = b64.b64decode(account_data_b64)

        if len(data) < 80:
            raise MeteoraPositionError(f"Position account data too short ({len(data)} bytes, need >= 80)")

        # Parse PositionV2 fields
        lb_pair = Pubkey.from_bytes(data[8:40])
        owner = Pubkey.from_bytes(data[40:72])
        lower_bin_id = struct.unpack_from("<i", data, 72)[0]
        upper_bin_id = struct.unpack_from("<i", data, 76)[0]

        logger.info(f"Fetched position state: {position_address[:8]}..., bins=[{lower_bin_id}, {upper_bin_id}]")

        return MeteoraPosition(
            position_address=position_address,
            lb_pair=str(lb_pair),
            owner=str(owner),
            lower_bin_id=lower_bin_id,
            upper_bin_id=upper_bin_id,
        )

    # =========================================================================
    # Instruction builders
    # =========================================================================

    def build_initialize_position_ix(
        self,
        lb_pair: Pubkey,
        position_kp: Keypair,
        lower_bin_id: int,
        width: int,
    ) -> Instruction:
        """Build initializePosition instruction.

        Args:
            lb_pair: Pool address.
            position_kp: New keypair for the position account.
            lower_bin_id: Lower bin ID.
            width: Number of bins in the position.

        Returns:
            Solana instruction.
        """
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)
        rent = Pubkey.from_string(RENT_SYSVAR_ID)
        event_authority = self.get_event_authority_pda()

        ix_data = INITIALIZE_POSITION_DISCRIMINATOR + struct.pack("<ii", lower_bin_id, width)

        accounts = [
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # payer
            AccountMeta(position_kp.pubkey(), is_signer=True, is_writable=True),  # position
            AccountMeta(lb_pair, is_signer=False, is_writable=False),  # lb_pair
            AccountMeta(self._owner, is_signer=False, is_writable=False),  # owner
            AccountMeta(system_program, is_signer=False, is_writable=False),  # system_program
            AccountMeta(rent, is_signer=False, is_writable=False),  # rent
            AccountMeta(event_authority, is_signer=False, is_writable=False),  # event_authority
            AccountMeta(self._program_id, is_signer=False, is_writable=False),  # program
        ]

        return Instruction(self._program_id, ix_data, accounts)

    def build_add_liquidity_by_strategy_ix(
        self,
        pool: MeteoraPool,
        position: Pubkey,
        lower_bin_id: int,
        upper_bin_id: int,
        amount_x: int,
        amount_y: int,
        active_id: int,
        max_active_bin_slippage: int = 5,
        strategy_type: int = STRATEGY_TYPE_SPOT_BALANCED,
    ) -> Instruction:
        """Build addLiquidityByStrategy instruction.

        LiquidityParameterByStrategy layout:
            amount_x: u64
            amount_y: u64
            active_id: i32
            max_active_bin_slippage: i32
            strategy_parameters:
                min_bin_id: i32
                max_bin_id: i32
                strategy_type: u8
                parameters: [u8; 64]  (zeroed for SpotBalanced)

        Args:
            pool: Pool information.
            position: Position account address.
            lower_bin_id: Min bin ID for strategy parameters.
            upper_bin_id: Max bin ID for strategy parameters.
            amount_x: Amount of token X in smallest units.
            amount_y: Amount of token Y in smallest units.
            active_id: Current active bin ID.
            max_active_bin_slippage: Max slippage in bins.
            strategy_type: Strategy type (default: SpotBalanced=6).

        Returns:
            Solana instruction.
        """
        lb_pair = Pubkey.from_string(pool.address)
        mint_x = Pubkey.from_string(pool.mint_x)
        mint_y = Pubkey.from_string(pool.mint_y)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        event_authority = self.get_event_authority_pda()

        # Compute accounts
        user_token_x = self._get_ata(self._owner, mint_x)
        user_token_y = self._get_ata(self._owner, mint_y)
        vault_x = Pubkey.from_string(pool.vault_x) if pool.vault_x else self._get_ata(lb_pair, mint_x)
        vault_y = Pubkey.from_string(pool.vault_y) if pool.vault_y else self._get_ata(lb_pair, mint_y)
        oracle = self.get_oracle_pda(lb_pair)

        # Bin array PDAs (need to cover the position's bin range)
        bin_arrays = self._get_bin_array_accounts(lb_pair, lower_bin_id, upper_bin_id)

        # Pack instruction data: LiquidityParameterByStrategy
        # amount_x(u64) + amount_y(u64) + active_id(i32) + max_slippage(i32) +
        # min_bin_id(i32) + max_bin_id(i32) + strategy_type(u8) + parameters([u8;64])
        parameters = bytes(64)  # Zeroed for SpotBalanced
        ix_data = (
            ADD_LIQUIDITY_BY_STRATEGY_DISCRIMINATOR
            + struct.pack(
                "<QQiiiiB",
                amount_x,
                amount_y,
                active_id,
                max_active_bin_slippage,
                lower_bin_id,
                upper_bin_id,
                strategy_type,
            )
            + parameters
        )

        accounts = [
            AccountMeta(position, is_signer=False, is_writable=True),  # position
            AccountMeta(lb_pair, is_signer=False, is_writable=True),  # lb_pair
        ]
        # Bin arrays (writable)
        for ba in bin_arrays:
            accounts.append(AccountMeta(ba, is_signer=False, is_writable=True))
        accounts.extend(
            [
                AccountMeta(user_token_x, is_signer=False, is_writable=True),  # user_token_x
                AccountMeta(user_token_y, is_signer=False, is_writable=True),  # user_token_y
                AccountMeta(vault_x, is_signer=False, is_writable=True),  # reserve_x
                AccountMeta(vault_y, is_signer=False, is_writable=True),  # reserve_y
                AccountMeta(mint_x, is_signer=False, is_writable=False),  # token_x_mint
                AccountMeta(mint_y, is_signer=False, is_writable=False),  # token_y_mint
                AccountMeta(oracle, is_signer=False, is_writable=True),  # oracle
                AccountMeta(self._owner, is_signer=True, is_writable=False),  # sender
                AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program_x
                AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program_y
                AccountMeta(event_authority, is_signer=False, is_writable=False),  # event_authority
                AccountMeta(self._program_id, is_signer=False, is_writable=False),  # program
            ]
        )

        return Instruction(self._program_id, ix_data, accounts)

    def build_remove_liquidity_by_range_ix(
        self,
        pool: MeteoraPool,
        position: Pubkey,
        from_bin_id: int,
        to_bin_id: int,
        bps_to_remove: int = 10000,
    ) -> Instruction:
        """Build removeLiquidityByRange instruction.

        Args:
            pool: Pool information.
            position: Position account address.
            from_bin_id: Starting bin ID.
            to_bin_id: Ending bin ID (inclusive).
            bps_to_remove: Basis points of liquidity to remove (10000 = 100%).

        Returns:
            Solana instruction.
        """
        lb_pair = Pubkey.from_string(pool.address)
        mint_x = Pubkey.from_string(pool.mint_x)
        mint_y = Pubkey.from_string(pool.mint_y)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        event_authority = self.get_event_authority_pda()

        user_token_x = self._get_ata(self._owner, mint_x)
        user_token_y = self._get_ata(self._owner, mint_y)
        vault_x = Pubkey.from_string(pool.vault_x) if pool.vault_x else self._get_ata(lb_pair, mint_x)
        vault_y = Pubkey.from_string(pool.vault_y) if pool.vault_y else self._get_ata(lb_pair, mint_y)
        oracle = self.get_oracle_pda(lb_pair)

        bin_arrays = self._get_bin_array_accounts(lb_pair, from_bin_id, to_bin_id)

        # Pack: from_bin_id(i32) + to_bin_id(i32) + bps_to_remove(u16)
        ix_data = REMOVE_LIQUIDITY_BY_RANGE_DISCRIMINATOR + struct.pack("<iiH", from_bin_id, to_bin_id, bps_to_remove)

        accounts = [
            AccountMeta(position, is_signer=False, is_writable=True),  # position
            AccountMeta(lb_pair, is_signer=False, is_writable=True),  # lb_pair
        ]
        for ba in bin_arrays:
            accounts.append(AccountMeta(ba, is_signer=False, is_writable=True))
        accounts.extend(
            [
                AccountMeta(user_token_x, is_signer=False, is_writable=True),  # user_token_x
                AccountMeta(user_token_y, is_signer=False, is_writable=True),  # user_token_y
                AccountMeta(vault_x, is_signer=False, is_writable=True),  # reserve_x
                AccountMeta(vault_y, is_signer=False, is_writable=True),  # reserve_y
                AccountMeta(mint_x, is_signer=False, is_writable=False),  # token_x_mint
                AccountMeta(mint_y, is_signer=False, is_writable=False),  # token_y_mint
                AccountMeta(oracle, is_signer=False, is_writable=True),  # oracle
                AccountMeta(self._owner, is_signer=True, is_writable=False),  # sender
                AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program_x
                AccountMeta(token_program, is_signer=False, is_writable=False),  # token_program_y
                AccountMeta(event_authority, is_signer=False, is_writable=False),  # event_authority
                AccountMeta(self._program_id, is_signer=False, is_writable=False),  # program
            ]
        )

        return Instruction(self._program_id, ix_data, accounts)

    def build_close_position_ix(
        self,
        lb_pair: Pubkey,
        position: Pubkey,
    ) -> Instruction:
        """Build closePosition instruction.

        Args:
            lb_pair: Pool address.
            position: Position account address.

        Returns:
            Solana instruction.
        """
        event_authority = self.get_event_authority_pda()

        ix_data = CLOSE_POSITION_DISCRIMINATOR

        accounts = [
            AccountMeta(position, is_signer=False, is_writable=True),  # position
            AccountMeta(lb_pair, is_signer=False, is_writable=False),  # lb_pair
            AccountMeta(self._owner, is_signer=True, is_writable=True),  # sender (rent_receiver)
            AccountMeta(event_authority, is_signer=False, is_writable=False),  # event_authority
            AccountMeta(self._program_id, is_signer=False, is_writable=False),  # program
        ]

        return Instruction(self._program_id, ix_data, accounts)

    # =========================================================================
    # ATA setup helpers
    # =========================================================================

    def _build_ata_setup_instructions(self, pool: MeteoraPool, amount_x_lamports: int) -> list[Instruction]:
        """Build instructions to create ATAs and wrap SOL if token X is WSOL."""
        from solders.system_program import TransferParams, transfer

        ixs: list[Instruction] = []
        mint_x = Pubkey.from_string(pool.mint_x)
        mint_y = Pubkey.from_string(pool.mint_y)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        ata_program = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
        system_program = Pubkey.from_string(SYSTEM_PROGRAM_ID)

        # Create ATAs (idempotent)
        for mint in [mint_x, mint_y]:
            user_ata = self._get_ata(self._owner, mint)
            ixs.append(
                Instruction(
                    ata_program,
                    bytes([1]),  # CreateIdempotent
                    [
                        AccountMeta(self._owner, is_signer=True, is_writable=True),
                        AccountMeta(user_ata, is_signer=False, is_writable=True),
                        AccountMeta(self._owner, is_signer=False, is_writable=False),
                        AccountMeta(mint, is_signer=False, is_writable=False),
                        AccountMeta(system_program, is_signer=False, is_writable=False),
                        AccountMeta(token_program, is_signer=False, is_writable=False),
                    ],
                )
            )

        # If token X is WSOL, transfer native SOL and sync
        if pool.mint_x == WSOL_MINT and amount_x_lamports > 0:
            user_ata_x = self._get_ata(self._owner, mint_x)
            ixs.append(
                transfer(
                    TransferParams(
                        from_pubkey=self._owner,
                        to_pubkey=user_ata_x,
                        lamports=amount_x_lamports,
                    )
                )
            )
            ixs.append(
                Instruction(
                    token_program,
                    bytes([17]),  # SyncNative
                    [AccountMeta(user_ata_x, is_signer=False, is_writable=True)],
                )
            )

        return ixs

    # =========================================================================
    # High-level transaction builders
    # =========================================================================

    def build_open_position_transaction(
        self,
        pool: MeteoraPool,
        lower_bin_id: int,
        upper_bin_id: int,
        amount_x: int,
        amount_y: int,
        slippage_bps: int = 100,
        strategy_type: int = STRATEGY_TYPE_SPOT_BALANCED,
    ) -> tuple[list[Instruction], Keypair, dict[str, Any]]:
        """Build a complete open position transaction.

        Creates initializePosition + addLiquidityByStrategy instructions.

        Args:
            pool: Pool information.
            lower_bin_id: Lower bin ID.
            upper_bin_id: Upper bin ID.
            amount_x: Amount of token X in smallest units.
            amount_y: Amount of token Y in smallest units.
            slippage_bps: Slippage in basis points (default 100 = 1%).
            strategy_type: Strategy type (default: SpotBalanced).

        Returns:
            Tuple of (instructions, position_keypair, metadata).
        """
        lb_pair = Pubkey.from_string(pool.address)
        width = upper_bin_id - lower_bin_id + 1

        # Generate a new keypair for the position account
        position_kp = Keypair()
        position_pubkey = position_kp.pubkey()

        # Apply slippage
        slippage_mult = 1 + (slippage_bps / 10000)
        amount_x_max = int(amount_x * slippage_mult)
        amount_y_max = int(amount_y * slippage_mult)

        # Build ATA setup
        pre_ixs = self._build_ata_setup_instructions(pool, amount_x_max)

        # Build initializePosition
        init_ix = self.build_initialize_position_ix(
            lb_pair=lb_pair,
            position_kp=position_kp,
            lower_bin_id=lower_bin_id,
            width=width,
        )

        # Build addLiquidityByStrategy
        add_liq_ix = self.build_add_liquidity_by_strategy_ix(
            pool=pool,
            position=position_pubkey,
            lower_bin_id=lower_bin_id,
            upper_bin_id=upper_bin_id,
            amount_x=amount_x_max,
            amount_y=amount_y_max,
            active_id=pool.active_bin_id,
            strategy_type=strategy_type,
        )

        ixs = pre_ixs + [init_ix, add_liq_ix]

        metadata = {
            "lower_bin_id": lower_bin_id,
            "upper_bin_id": upper_bin_id,
            "width": width,
            "amount_x_max": amount_x_max,
            "amount_y_max": amount_y_max,
            "position_address": str(position_pubkey),
            "active_bin_id": pool.active_bin_id,
            "bin_step": pool.bin_step,
            "slippage_bps": slippage_bps,
            "strategy_type": strategy_type,
        }

        logger.info(
            f"Built Meteora open position: pool={pool.address[:8]}..., "
            f"bins=[{lower_bin_id}, {upper_bin_id}], width={width}"
        )

        return ixs, position_kp, metadata

    def build_close_position_transaction(
        self,
        pool: MeteoraPool,
        position: MeteoraPosition,
    ) -> tuple[list[Instruction], dict[str, Any]]:
        """Build instructions to fully close a position.

        removeLiquidityByRange (100%) + closePosition.

        Args:
            pool: Pool information.
            position: Position to close.

        Returns:
            Tuple of (instructions, metadata).
        """
        lb_pair = Pubkey.from_string(pool.address)
        position_pubkey = Pubkey.from_string(position.position_address)

        all_ixs: list[Instruction] = []

        # Remove all liquidity if position has bins
        if position.lower_bin_id != position.upper_bin_id or position.total_x > 0 or position.total_y > 0:
            remove_ix = self.build_remove_liquidity_by_range_ix(
                pool=pool,
                position=position_pubkey,
                from_bin_id=position.lower_bin_id,
                to_bin_id=position.upper_bin_id,
                bps_to_remove=10000,  # 100%
            )
            all_ixs.append(remove_ix)

        # Close position
        close_ix = self.build_close_position_ix(
            lb_pair=lb_pair,
            position=position_pubkey,
        )
        all_ixs.append(close_ix)

        metadata = {
            "position_address": position.position_address,
            "pool": pool.address,
            "lower_bin_id": position.lower_bin_id,
            "upper_bin_id": position.upper_bin_id,
        }

        logger.info(f"Built Meteora close position: {position.position_address[:8]}...")

        return all_ixs, metadata

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _get_bin_array_accounts(self, lb_pair: Pubkey, lower_bin_id: int, upper_bin_id: int) -> list[Pubkey]:
        """Get bin array PDAs covering the given bin range.

        A position's bin range may span 1-3 bin arrays (each covers 70 bins).
        Returns the unique bin array PDAs in order.
        """
        lower_idx = get_bin_array_index(lower_bin_id)
        upper_idx = get_bin_array_index(upper_bin_id)

        bin_arrays = []
        for idx in range(lower_idx, upper_idx + 1):
            pda = get_bin_array_pda(self._program_id, lb_pair, idx)
            bin_arrays.append(pda)

        return bin_arrays
