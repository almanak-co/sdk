"""Solana Fork Manager — local testing environment using solana-test-validator.

Analogous to RollingForkManager for EVM/Anvil. Manages a solana-test-validator
subprocess that clones accounts from mainnet-beta for local strategy testing.

Usage:
    fork_manager = SolanaForkManager(
        rpc_url="https://api.mainnet-beta.solana.com",
        validator_port=8899,
    )

    await fork_manager.start()
    await fork_manager.fund_wallet("Hs5wSP...", sol_amount=Decimal("10"))
    await fork_manager.fund_tokens("Hs5wSP...", {"USDC": Decimal("1000")})

    # ... run tests or strategy ...

    await fork_manager.stop()

Key differences from Anvil:
- No full state fork: must explicitly clone each account/program
- No setBalance/setStorageAt: use requestAirdrop for SOL, mint authority trick for SPL tokens
- No impersonation or snapshot/revert
- Slots advance automatically (~400ms each)
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import socket
import struct
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Solana Constants
# =============================================================================

# Well-known program IDs that should always be available on test-validator
# (most are built-in, but some need explicit cloning)
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
MEMO_PROGRAM = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"

# Jupiter Aggregator v6 program
JUPITER_PROGRAM = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

# WSOL special mint
WSOL_MINT = "So11111111111111111111111111111111111111112"

# Common Solana token mints
SOLANA_TOKEN_MINTS: dict[str, str] = {
    "SOL": WSOL_MINT,
    "WSOL": WSOL_MINT,
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "JITOSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
}

# Decimals for common Solana tokens
SOLANA_TOKEN_DECIMALS: dict[str, int] = {
    "SOL": 9,
    "WSOL": 9,
    "USDC": 6,
    "USDT": 6,
    "JUP": 6,
    "RAY": 6,
    "ORCA": 6,
    "BONK": 5,
    "MSOL": 9,
    "JITOSOL": 9,
}

# Default accounts to clone from mainnet for basic swap testing
DEFAULT_CLONE_ACCOUNTS: list[str] = [
    # USDC mint
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    # USDT mint
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
]

DEFAULT_CLONE_PROGRAMS: list[str] = [
    # Jupiter v6 (upgradeable, so we use --clone-upgradeable-program)
    JUPITER_PROGRAM,
]

# SPL Token Mint account layout (82 bytes)
# https://github.com/solana-labs/solana-program-library/blob/master/token/program/src/state.rs
MINT_LAYOUT_SIZE = 82
MINT_AUTHORITY_OFFSET = 0  # COption<Pubkey>: 4-byte discriminator + 32-byte pubkey
MINT_AUTHORITY_PUBKEY_OFFSET = 4
MINT_SUPPLY_OFFSET = 36
MINT_DECIMALS_OFFSET = 44
MINT_IS_INITIALIZED_OFFSET = 45
MINT_FREEZE_AUTHORITY_OFFSET = 46


@dataclass
class SolanaForkManager:
    """Manages a solana-test-validator subprocess for local Solana testing.

    Provides the same lifecycle contract as RollingForkManager:
    start() -> fund_wallet() / fund_tokens() -> stop()

    Args:
        rpc_url: Solana RPC URL to clone accounts from (mainnet-beta or devnet).
        validator_port: Port for the local test-validator JSON-RPC (default: 8899).
        faucet_port: Port for the local faucet (default: 9900).
        startup_timeout_seconds: Max seconds to wait for validator startup.
        clone_accounts: Additional account addresses to clone from source.
        clone_programs: Additional upgradeable program addresses to clone.
        ledger_dir: Directory for validator ledger. None = auto temp dir.
        mint_keypair_path: Path to keypair that becomes mint authority for token funding.
    """

    rpc_url: str
    validator_port: int = 8899
    faucet_port: int = 9900
    startup_timeout_seconds: float = 60.0
    clone_accounts: list[str] = field(default_factory=list)
    clone_programs: list[str] = field(default_factory=list)
    ledger_dir: str | None = None
    mint_keypair_path: str | None = None

    # Internal state
    _process: subprocess.Popen | None = field(default=None, init=False, repr=False)
    _is_running: bool = field(default=False, init=False, repr=False)
    _current_slot: int | None = field(default=None, init=False, repr=False)
    _start_time: float | None = field(default=None, init=False, repr=False)
    _temp_dir: str | None = field(default=None, init=False, repr=False)
    _mint_authority_keypair: Any | None = field(default=None, init=False, repr=False)
    _modified_mint_dir: str | None = field(default=None, init=False, repr=False)

    @property
    def is_running(self) -> bool:
        """Check if the test-validator is currently running."""
        return self._is_running

    @property
    def current_slot(self) -> int | None:
        """Get current validator slot number."""
        return self._current_slot

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> bool:
        """Start the solana-test-validator fork.

        Clones accounts from the configured RPC URL, starts the validator,
        and waits for it to be ready.

        Returns:
            True if validator started successfully, False otherwise.
        """
        if self._is_running:
            logger.warning("Solana test-validator is already running")
            return True

        try:
            # Prepare temp dirs for modified mint accounts
            self._temp_dir = tempfile.mkdtemp(prefix="solana_fork_")
            self._modified_mint_dir = os.path.join(self._temp_dir, "accounts")
            os.makedirs(self._modified_mint_dir, exist_ok=True)

            # Generate or load mint authority keypair
            await self._prepare_mint_authority()

            # Fetch and modify mint accounts (replace authority with ours)
            await self._prepare_modified_mints()

            # Build validator command
            cmd = self._build_validator_command()

            logger.info(
                f"Starting solana-test-validator: port={self.validator_port}, source={self._mask_url(self.rpc_url)}"
            )
            logger.debug(f"Validator command: {' '.join(cmd)}")

            # Start process
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for ready
            ready = await self._wait_for_ready()
            if not ready:
                logger.error("solana-test-validator startup timed out")
                await self.stop()
                return False

            self._is_running = True
            self._start_time = time.time()

            # Get current slot
            self._current_slot = await self._get_slot()

            logger.info(f"solana-test-validator started: port={self.validator_port}, slot={self._current_slot}")
            return True

        except FileNotFoundError:
            logger.error(
                "solana-test-validator not found. Install Solana CLI tools: "
                'sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"'
            )
            return False
        except Exception as e:
            logger.exception(f"Failed to start solana-test-validator: {e}")
            await self.stop()
            return False

    async def stop(self) -> None:
        """Stop the solana-test-validator and clean up resources."""
        if self._process is not None:
            try:
                self._process.terminate()
                try:
                    await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(None, self._process.wait),
                        timeout=5.0,
                    )
                except TimeoutError:
                    logger.warning("solana-test-validator did not terminate, killing")
                    self._process.kill()
                    self._process.wait()
            except Exception as e:
                logger.warning(f"Error stopping solana-test-validator: {e}")
            finally:
                self._process = None

        self._is_running = False
        self._current_slot = None
        self._start_time = None

        # Wait for port to be freed
        await self._wait_for_port_free(timeout=5.0)

        # Clean up temp dirs
        if self._temp_dir and os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

        logger.info("solana-test-validator stopped")

    async def reset(self) -> bool:
        """Reset the validator by stopping and restarting.

        Unlike Anvil, solana-test-validator has no in-place reset;
        we must restart the process entirely.

        Returns:
            True if reset successful, False otherwise.
        """
        await self.stop()
        return await self.start()

    # =========================================================================
    # Wallet Funding
    # =========================================================================

    async def fund_wallet(self, address: str, sol_amount: Decimal) -> bool:
        """Fund a wallet with SOL via requestAirdrop.

        Args:
            address: Solana wallet public key (base58).
            sol_amount: Amount of SOL to airdrop.

        Returns:
            True if airdrop succeeded.
        """
        if not self._is_running:
            logger.error("Cannot fund wallet: validator not running")
            return False

        lamports = int(sol_amount * Decimal("1000000000"))
        logger.info(f"Airdropping {sol_amount} SOL ({lamports} lamports) to {address}")

        try:
            sig = await self._rpc_call("requestAirdrop", [address, lamports])
            if sig:
                await self._confirm_transaction(sig)
                balance = await self._get_sol_balance(address)
                logger.info(f"Airdrop complete. Balance: {balance} lamports")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to airdrop SOL: {e}")
            return False

    async def fund_tokens(
        self,
        address: str,
        tokens: dict[str, Decimal],
    ) -> bool:
        """Fund a wallet with SPL tokens.

        Uses the mint authority trick: at startup, we cloned mints with our
        keypair as authority. Now we create ATAs and mint tokens directly.

        Args:
            address: Solana wallet public key (base58).
            tokens: Dict of token symbol -> amount (human-readable).

        Returns:
            True if all tokens funded, False if any fail.
        """
        if not self._is_running:
            logger.error("Cannot fund tokens: validator not running")
            return False

        if self._mint_authority_keypair is None:
            logger.error("Mint authority keypair not available")
            return False

        # Ensure mint authority has SOL for transaction fees (once, not per-token)
        authority_pubkey = str(self._mint_authority_keypair.pubkey())
        authority_balance = await self._get_sol_balance(authority_pubkey)
        if authority_balance < 100_000_000:  # < 0.1 SOL
            airdrop_sig = await self._rpc_call("requestAirdrop", [authority_pubkey, 2_000_000_000])
            if airdrop_sig:
                await self._confirm_transaction(airdrop_sig)

        all_success = True
        for symbol, amount in tokens.items():
            try:
                success = await self._fund_single_token(address, symbol, amount)
                if not success:
                    logger.error(f"Failed to fund {symbol}")
                    all_success = False
            except Exception as e:
                logger.error(f"Error funding {symbol}: {e}")
                all_success = False

        return all_success

    async def _fund_single_token(self, address: str, symbol: str, amount: Decimal) -> bool:
        """Fund a single SPL token by creating ATA and minting."""
        mint = SOLANA_TOKEN_MINTS.get(symbol.upper())
        if not mint:
            logger.error(f"Unknown token symbol: {symbol}")
            return False

        decimals = SOLANA_TOKEN_DECIMALS.get(symbol.upper())
        if decimals is None:
            logger.error(f"Unknown decimals for {symbol}")
            return False

        raw_amount = int(amount * Decimal(10**decimals))
        logger.info(f"Funding {amount} {symbol} ({raw_amount} raw) to {address}")

        try:
            from solders.pubkey import Pubkey

            mint_pubkey = Pubkey.from_string(mint)
            owner_pubkey = Pubkey.from_string(address)

            # Derive ATA address
            ata = self._derive_ata(owner_pubkey, mint_pubkey)

            # Check if ATA exists
            ata_info = await self._rpc_call(
                "getAccountInfo",
                [str(ata), {"encoding": "base64", "commitment": "confirmed"}],
            )

            if ata_info is None or ata_info.get("value") is None:
                # Create ATA via transaction
                success = await self._create_ata_and_mint(
                    owner=owner_pubkey,
                    mint=mint_pubkey,
                    ata=ata,
                    amount=raw_amount,
                )
            else:
                # ATA exists, just mint
                success = await self._mint_to(
                    mint=mint_pubkey,
                    destination=ata,
                    amount=raw_amount,
                )

            if success:
                # Poll balance until non-zero (tx may take a few slots to process)
                balance = "0"
                for _ in range(20):
                    balance = await self._get_token_balance(str(ata))
                    if balance != "0":
                        break
                    await asyncio.sleep(0.5)
                logger.info(f"Token balance after funding: {balance} {symbol}")
                if balance == "0":
                    logger.warning(f"Balance still 0 for {symbol} after polling — tx may have failed")
                    return False

            return success

        except ImportError:
            logger.error("solders package required for token funding. Install with: pip install solders")
            return False
        except Exception as e:
            logger.error(f"Error funding {symbol}: {e}")
            return False

    # =========================================================================
    # Public Getters
    # =========================================================================

    def get_rpc_url(self) -> str:
        """Get the local RPC URL for the test-validator."""
        return f"http://127.0.0.1:{self.validator_port}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize manager state for logging."""
        return {
            "rpc_url": self._mask_url(self.rpc_url),
            "validator_port": self.validator_port,
            "is_running": self._is_running,
            "current_slot": self._current_slot,
            "fork_rpc_url": self.get_rpc_url(),
        }

    # =========================================================================
    # Internal: Validator Command
    # =========================================================================

    def _build_validator_command(self) -> list[str]:
        """Build the solana-test-validator command."""
        ledger = self.ledger_dir or os.path.join(self._temp_dir or "/tmp", "test-ledger")

        cmd = [
            "solana-test-validator",
            "--rpc-port",
            str(self.validator_port),
            "--faucet-port",
            str(self.faucet_port),
            "--ledger",
            ledger,
            "--quiet",
            "--reset",
        ]

        # Collect modified mint addresses so we don't --clone them
        # (--account overrides must not conflict with --clone for same address)
        modified_mint_addresses: set[str] = set()
        if self._modified_mint_dir and os.path.isdir(self._modified_mint_dir):
            for filename in os.listdir(self._modified_mint_dir):
                if filename.endswith(".json"):
                    modified_mint_addresses.add(filename.replace(".json", ""))

        # Source RPC for cloning (--url is global, passed once)
        has_clones = False

        # Clone accounts from mainnet (skip any we're overriding with --account)
        all_clone_accounts = list(DEFAULT_CLONE_ACCOUNTS) + list(self.clone_accounts)
        for account in all_clone_accounts:
            if account not in modified_mint_addresses:
                cmd.extend(["--clone", account])
                has_clones = True

        # Clone upgradeable programs from mainnet
        all_clone_programs = list(DEFAULT_CLONE_PROGRAMS) + list(self.clone_programs)
        for program in all_clone_programs:
            cmd.extend(["--clone-upgradeable-program", program])
            has_clones = True

        # --url must come once (global) if any cloning is needed
        if has_clones:
            cmd.extend(["--url", self.rpc_url])

        # Load modified mint accounts (with our authority)
        for mint_address in sorted(modified_mint_addresses):
            assert self._modified_mint_dir is not None  # set during start()
            filepath = os.path.join(self._modified_mint_dir, f"{mint_address}.json")
            cmd.extend(["--account", mint_address, filepath])

        return cmd

    # =========================================================================
    # Internal: Mint Authority Preparation
    # =========================================================================

    async def _prepare_mint_authority(self) -> None:
        """Generate or load the keypair that will become mint authority."""
        try:
            from solders.keypair import Keypair

            if self.mint_keypair_path and os.path.exists(self.mint_keypair_path):
                with open(self.mint_keypair_path) as f:
                    secret = json.load(f)
                self._mint_authority_keypair = Keypair.from_bytes(bytes(secret))
            else:
                # Generate a fresh keypair for this session
                self._mint_authority_keypair = Keypair()
                logger.debug(f"Generated mint authority: {self._mint_authority_keypair.pubkey()}")
        except ImportError:
            logger.warning("solders not installed — token funding will be unavailable")
            self._mint_authority_keypair = None

    async def _prepare_modified_mints(self) -> None:
        """Fetch mint accounts from mainnet and replace authority with ours.

        For each token we want to fund, we fetch the mint account data,
        replace the mint authority with our generated keypair, and save
        as a JSON file to load via --account flag.
        """
        if self._mint_authority_keypair is None:
            return

        authority_bytes = bytes(self._mint_authority_keypair.pubkey())

        for symbol, mint_address in SOLANA_TOKEN_MINTS.items():
            if symbol in ("SOL", "WSOL"):
                continue  # Native SOL doesn't need mint authority

            try:
                # Fetch mint account from mainnet
                account_info = await self._rpc_call_to_url(
                    self.rpc_url,
                    "getAccountInfo",
                    [mint_address, {"encoding": "base64"}],
                )

                if not account_info or not account_info.get("value"):
                    logger.debug(f"Could not fetch mint for {symbol}, skipping")
                    continue

                value = account_info["value"]
                data_b64 = value["data"][0]
                data = bytearray(base64.b64decode(data_b64))

                if len(data) < MINT_LAYOUT_SIZE:
                    logger.warning(f"Mint account {symbol} data too small ({len(data)} bytes)")
                    continue

                # Replace mint authority (bytes 4-36) with our keypair
                # First check the COption discriminator (bytes 0-3)
                has_authority = struct.unpack_from("<I", data, 0)[0]
                if has_authority == 0:
                    # No authority set — set it to ours
                    struct.pack_into("<I", data, 0, 1)  # COption::Some

                # Write our authority pubkey
                data[MINT_AUTHORITY_PUBKEY_OFFSET : MINT_AUTHORITY_PUBKEY_OFFSET + 32] = authority_bytes

                # Also replace freeze authority with ours (optional, prevents issues)
                freeze_discriminator = struct.unpack_from("<I", data, MINT_FREEZE_AUTHORITY_OFFSET)[0]
                if freeze_discriminator == 1:
                    data[MINT_FREEZE_AUTHORITY_OFFSET + 4 : MINT_FREEZE_AUTHORITY_OFFSET + 36] = authority_bytes

                # Build the account JSON in Solana CLI format
                owner = value.get("owner", TOKEN_PROGRAM)
                lamports = value.get("lamports", 1461600)
                space = value.get("space", len(data))
                executable = value.get("executable", False)
                rent_epoch = value.get("rentEpoch", 0)

                account_json = {
                    "pubkey": mint_address,
                    "account": {
                        "lamports": lamports,
                        "data": [
                            base64.b64encode(bytes(data)).decode("ascii"),
                            "base64",
                        ],
                        "owner": owner,
                        "executable": executable,
                        "rentEpoch": rent_epoch,
                        "space": space,
                    },
                }

                # Write to file
                assert self._modified_mint_dir is not None  # set during start()
                filepath = os.path.join(self._modified_mint_dir, f"{mint_address}.json")
                with open(filepath, "w") as f:
                    json.dump(account_json, f)

                logger.debug(f"Prepared modified mint for {symbol}: {mint_address}")

            except Exception as e:
                logger.warning(f"Failed to prepare mint for {symbol}: {e}")

    # =========================================================================
    # Internal: Token Operations (post-startup)
    # =========================================================================

    def _derive_ata(self, owner: Any, mint: Any) -> Any:
        """Derive the Associated Token Account address."""
        from solders.pubkey import Pubkey

        ata, _bump = Pubkey.find_program_address(
            [bytes(owner), bytes(Pubkey.from_string(TOKEN_PROGRAM)), bytes(mint)],
            Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM),
        )
        return ata

    async def _create_ata_and_mint(self, owner: Any, mint: Any, ata: Any, amount: int) -> bool:
        """Create an ATA and mint tokens to it in a single transaction."""
        try:
            from solders.instruction import AccountMeta, Instruction
            from solders.message import MessageV0
            from solders.pubkey import Pubkey
            from solders.transaction import VersionedTransaction

            authority = self._mint_authority_keypair
            if authority is None:
                logger.error("Mint authority keypair not initialized")
                return False

            # Build Create ATA instruction
            # CreateAssociatedTokenAccount instruction
            create_ata_ix = Instruction(
                program_id=Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM),
                accounts=[
                    AccountMeta(authority.pubkey(), is_signer=True, is_writable=True),  # payer
                    AccountMeta(ata, is_signer=False, is_writable=True),  # ata
                    AccountMeta(owner, is_signer=False, is_writable=False),  # owner
                    AccountMeta(mint, is_signer=False, is_writable=False),  # mint
                    AccountMeta(Pubkey.from_string(SYSTEM_PROGRAM), is_signer=False, is_writable=False),
                    AccountMeta(Pubkey.from_string(TOKEN_PROGRAM), is_signer=False, is_writable=False),
                ],
                data=b"",
            )

            # Build MintTo instruction
            # MintTo instruction index = 7
            mint_data = bytearray([7])
            mint_data.extend(struct.pack("<Q", amount))

            mint_to_ix = Instruction(
                program_id=Pubkey.from_string(TOKEN_PROGRAM),
                accounts=[
                    AccountMeta(mint, is_signer=False, is_writable=True),  # mint
                    AccountMeta(ata, is_signer=False, is_writable=True),  # destination
                    AccountMeta(authority.pubkey(), is_signer=True, is_writable=False),  # authority
                ],
                data=bytes(mint_data),
            )

            # Get recent blockhash
            blockhash_resp = await self._rpc_call("getLatestBlockhash", [{"commitment": "confirmed"}])
            blockhash_str = blockhash_resp["value"]["blockhash"]

            from solders.hash import Hash

            blockhash = Hash.from_string(blockhash_str)

            # Build and sign transaction
            msg = MessageV0.try_compile(
                payer=authority.pubkey(),
                instructions=[create_ata_ix, mint_to_ix],
                address_lookup_table_accounts=[],
                recent_blockhash=blockhash,
            )
            tx = VersionedTransaction(msg, [authority])

            # Send
            tx_bytes = bytes(tx)
            tx_b64 = base64.b64encode(tx_bytes).decode("ascii")
            sig = await self._rpc_call(
                "sendTransaction",
                [tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}],
            )

            if sig:
                logger.debug(f"Create ATA + MintTo tx: {sig}")
                await self._confirm_transaction(sig)
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to create ATA and mint: {e}")
            return False

    async def _mint_to(self, mint: Any, destination: Any, amount: int) -> bool:
        """Mint tokens to an existing ATA."""
        try:
            from solders.instruction import AccountMeta, Instruction
            from solders.message import MessageV0
            from solders.pubkey import Pubkey
            from solders.transaction import VersionedTransaction

            authority = self._mint_authority_keypair
            if authority is None:
                logger.error("Mint authority keypair not initialized")
                return False

            # MintTo instruction (index = 7)
            mint_data = bytearray([7])
            mint_data.extend(struct.pack("<Q", amount))

            mint_to_ix = Instruction(
                program_id=Pubkey.from_string(TOKEN_PROGRAM),
                accounts=[
                    AccountMeta(mint, is_signer=False, is_writable=True),
                    AccountMeta(destination, is_signer=False, is_writable=True),
                    AccountMeta(authority.pubkey(), is_signer=True, is_writable=False),
                ],
                data=bytes(mint_data),
            )

            blockhash_resp = await self._rpc_call("getLatestBlockhash", [{"commitment": "confirmed"}])
            blockhash_str = blockhash_resp["value"]["blockhash"]

            from solders.hash import Hash

            blockhash = Hash.from_string(blockhash_str)

            msg = MessageV0.try_compile(
                payer=authority.pubkey(),
                instructions=[mint_to_ix],
                address_lookup_table_accounts=[],
                recent_blockhash=blockhash,
            )
            tx = VersionedTransaction(msg, [authority])
            tx_bytes = bytes(tx)
            tx_b64 = base64.b64encode(tx_bytes).decode("ascii")

            sig = await self._rpc_call(
                "sendTransaction",
                [tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}],
            )

            if sig:
                logger.debug(f"MintTo tx: {sig}")
                await self._confirm_transaction(sig)
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to mint tokens: {e}")
            return False

    # =========================================================================
    # Internal: RPC and Health Checks
    # =========================================================================

    async def _confirm_transaction(self, signature: str, timeout: float = 15.0) -> bool:
        """Wait for a transaction to reach 'confirmed' status."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                result = await self._rpc_call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
                statuses = result.get("value", []) if isinstance(result, dict) else []
                if statuses and statuses[0] is not None:
                    status = statuses[0]
                    if status.get("confirmationStatus") in ("confirmed", "finalized"):
                        return True
                    if status.get("err") is not None:
                        logger.warning(f"Transaction {signature[:16]}... failed: {status['err']}")
                        return False
            except Exception:
                pass
            await asyncio.sleep(0.3)
        logger.warning(f"Transaction {signature[:16]}... confirmation timed out after {timeout}s")
        return False

    async def _wait_for_ready(self) -> bool:
        """Wait for solana-test-validator to be ready."""
        start = time.time()

        while time.time() - start < self.startup_timeout_seconds:
            if self._is_port_open():
                try:
                    result = await self._rpc_call("getHealth", [])
                    if result == "ok":
                        return True
                except Exception:
                    pass

            # Check if process died
            if self._process is not None and self._process.poll() is not None:
                stdout = self._process.stdout.read() if self._process.stdout else b""
                stderr = self._process.stderr.read() if self._process.stderr else b""
                logger.error(
                    f"solana-test-validator exited unexpectedly. stdout: {stdout.decode()}, stderr: {stderr.decode()}"
                )
                return False

            await asyncio.sleep(0.5)

        return False

    async def _wait_for_port_free(self, timeout: float = 5.0) -> None:
        """Wait for the validator port to be freed."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", self.validator_port))
                    return
            except OSError:
                await asyncio.sleep(0.1)
        logger.warning(f"Port {self.validator_port} not freed after {timeout}s")

    def _is_port_open(self) -> bool:
        """Check if the validator port is accepting connections."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect(("127.0.0.1", self.validator_port))
                return True
        except (TimeoutError, ConnectionRefusedError, OSError):
            return False

    async def _rpc_call(self, method: str, params: list[Any]) -> Any:
        """Make a JSON-RPC call to the local test-validator."""
        return await self._rpc_call_to_url(self.get_rpc_url(), method, params)

    async def _rpc_call_to_url(self, url: str, method: str, params: list[Any]) -> Any:
        """Make a JSON-RPC call to an arbitrary URL."""
        import urllib.request

        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
        ).encode()

        def _do_request() -> Any:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                if "error" in data:
                    raise RuntimeError(f"RPC error: {data['error'].get('message', data['error'])}")
                return data.get("result")

        return await asyncio.get_event_loop().run_in_executor(None, _do_request)

    async def _get_slot(self) -> int | None:
        """Get current validator slot."""
        try:
            result = await self._rpc_call("getSlot", [])
            return int(result) if result is not None else None
        except Exception:
            return None

    async def _get_sol_balance(self, address: str) -> int:
        """Get SOL balance in lamports."""
        try:
            result = await self._rpc_call("getBalance", [address, {"commitment": "confirmed"}])
            return result.get("value", 0) if isinstance(result, dict) else 0
        except Exception:
            return 0

    async def _get_token_balance(self, ata_address: str) -> str:
        """Get SPL token balance for an ATA."""
        try:
            result = await self._rpc_call("getTokenAccountBalance", [ata_address, {"commitment": "confirmed"}])
            if result and "value" in result:
                return result["value"].get("uiAmountString", "0")
            return "0"
        except Exception:
            return "0"

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask sensitive parts of RPC URLs for logging."""
        if "://" in url:
            parts = url.split("/")
            if len(parts) > 3:
                return "/".join(parts[:3]) + "/***"
        return url
