#!/usr/bin/env python3
"""E2E test: LP open via Safe MultiSend on Anvil Base fork.

Verifies that the gas estimation fix in DirectSafeSigner prevents GS013
errors when opening LP positions through a Safe using MultiSend bundles.

This test:
  1. Starts an Anvil fork of Base mainnet + Gateway
  2. Adds EOA (Anvil default account) as Safe owner
  3. Drains pre-existing tokens from Safe (to avoid NAV conflicts)
  4. Funds Safe with ALMANAK + USDC + ETH
  5. Opens an LP position on Uniswap V3 (ALMANAK/USDC) via Safe MultiSend
  6. Verifies the transaction succeeds (no GS013 gas error)

Usage:
    uv run python tests/e2e_safe_lp_test.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import sys
import time
from decimal import Decimal

from web3 import Web3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
# Show gas estimation debug logs from Safe signer
logging.getLogger("almanak.framework.execution.signer.safe").setLevel(logging.DEBUG)

logger = logging.getLogger("e2e_safe_lp")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Anvil default account (matches ALMANAK_PRIVATE_KEY in .env)
EOA_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Base chain addresses
SAFE_ADDRESS = "0x98aE9CE2606e2773eE948178C3a163fdB8194c04"
ALMANAK_TOKEN = "0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3"
USDC_TOKEN = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL_ADDRESS = "0xbDbC38652D78AF0383322bBc823E06FA108d0874"
BURN_ADDRESS = "0x000000000000000000000000000000000000dEaD"

# ERC20 ABI (just what we need)
ERC20_ABI = json.loads("""[
  {"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},
  {"inputs":[],"name":"decimals","outputs":[{"type":"uint8"}],"stateMutability":"view","type":"function"},
  {"inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"}
]""")

# Safe ABI (addOwnerWithThreshold + getOwners)
SAFE_ABI = json.loads("""[
  {"inputs":[{"name":"owner","type":"address"},{"name":"_threshold","type":"uint256"}],"name":"addOwnerWithThreshold","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[],"name":"getOwners","outputs":[{"type":"address[]"}],"stateMutability":"view","type":"function"}
]""")


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Anvil helpers
# ---------------------------------------------------------------------------

def anvil_set_balance(w3: Web3, address: str, wei: int) -> None:
    w3.provider.make_request("anvil_setBalance", [address, hex(wei)])


def anvil_impersonate(w3: Web3, address: str) -> None:
    w3.provider.make_request("anvil_impersonateAccount", [address])


def anvil_stop_impersonate(w3: Web3, address: str) -> None:
    w3.provider.make_request("anvil_stopImpersonatingAccount", [address])


def erc20_balance(w3: Web3, token: str, account: str) -> int:
    contract = w3.eth.contract(address=w3.to_checksum_address(token), abi=ERC20_ABI)
    return contract.functions.balanceOf(w3.to_checksum_address(account)).call()


def erc20_decimals(w3: Web3, token: str) -> int:
    contract = w3.eth.contract(address=w3.to_checksum_address(token), abi=ERC20_ABI)
    return contract.functions.decimals().call()


def erc20_transfer(w3: Web3, token: str, sender: str, to: str, amount: int) -> None:
    """Transfer ERC20 tokens using Anvil impersonation."""
    anvil_set_balance(w3, sender, 10**18)  # Fund sender with ETH for gas
    anvil_impersonate(w3, sender)
    contract = w3.eth.contract(address=w3.to_checksum_address(token), abi=ERC20_ABI)
    tx = contract.functions.transfer(
        w3.to_checksum_address(to), amount
    ).build_transaction({
        "from": w3.to_checksum_address(sender),
        "gas": 100_000,
        "gasPrice": w3.eth.gas_price,
        "nonce": w3.eth.get_transaction_count(w3.to_checksum_address(sender)),
    })
    tx_hash = w3.eth.send_transaction(tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    assert receipt["status"] == 1, f"Transfer failed: {receipt}"
    anvil_stop_impersonate(w3, sender)


def add_eoa_as_safe_owner(w3: Web3, safe: str, eoa: str) -> None:
    """Add EOA as Safe owner with threshold=1 using Anvil impersonation."""
    safe_addr = w3.to_checksum_address(safe)
    safe_contract = w3.eth.contract(address=safe_addr, abi=SAFE_ABI)

    owners = safe_contract.functions.getOwners().call()
    if eoa.lower() in [o.lower() for o in owners]:
        logger.info("EOA %s already a Safe owner", eoa[:10])
        return

    # Impersonate Safe itself to call addOwnerWithThreshold
    anvil_impersonate(w3, safe)
    anvil_set_balance(w3, safe, 10**18)  # Give Safe ETH for gas

    tx = safe_contract.functions.addOwnerWithThreshold(
        w3.to_checksum_address(eoa), 1
    ).build_transaction({
        "from": safe_addr,
        "gas": 200_000,
        "gasPrice": w3.eth.gas_price,
        "nonce": w3.eth.get_transaction_count(safe_addr),
    })
    tx_hash = w3.eth.send_transaction(tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    assert receipt["status"] == 1, f"addOwnerWithThreshold failed: {receipt}"
    anvil_stop_impersonate(w3, safe)

    owners = safe_contract.functions.getOwners().call()
    logger.info("Safe owners after add: %s", [o[:10] + "..." for o in owners])


def drain_safe_tokens(w3: Web3, safe: str, tokens: list[str]) -> None:
    """Drain all ERC20 tokens from Safe to burn address (avoids NAV conflicts)."""
    safe_addr = w3.to_checksum_address(safe)
    anvil_impersonate(w3, safe)

    for token in tokens:
        bal = erc20_balance(w3, token, safe)
        if bal > 0:
            contract = w3.eth.contract(address=w3.to_checksum_address(token), abi=ERC20_ABI)
            tx = contract.functions.transfer(
                BURN_ADDRESS, bal
            ).build_transaction({
                "from": safe_addr,
                "gas": 100_000,
                "gasPrice": w3.eth.gas_price,
                "nonce": w3.eth.get_transaction_count(safe_addr),
            })
            tx_hash = w3.eth.send_transaction(tx)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            if receipt["status"] == 1:
                logger.info("Drained %d of %s from Safe", bal, token[:10])
            else:
                logger.warning("Failed to drain %s from Safe", token[:10])

    anvil_stop_impersonate(w3, safe)


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

async def main() -> bool:
    """Run the E2E test. Returns True if all assertions pass."""
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.managed import ManagedGateway, find_free_port as gw_find_free_port

    # 1. Start ManagedGateway with Anvil Base fork
    logger.info("=" * 60)
    logger.info("STEP 1: Starting Anvil Base fork + Gateway")
    logger.info("=" * 60)

    gw_port = gw_find_free_port()
    settings = GatewaySettings(
        grpc_port=gw_port,
        network="anvil",
        private_key=os.environ.get(
            "ALMANAK_PRIVATE_KEY",
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        ),
        # Safe integration: enable DirectSafeSigner for vault/LP operations
        safe_address=SAFE_ADDRESS,
        safe_mode="direct",
        metrics_enabled=False,
        metrics_port=find_free_port(),
    )

    gateway = ManagedGateway(
        settings,
        anvil_chains=["base"],
        wallet_address=EOA_WALLET,
    )

    gateway.start(timeout=60)
    logger.info("Gateway started on port %d", gw_port)

    try:
        # Wait for Anvil to be ready
        anvil_port = int(os.environ.get("ANVIL_BASE_PORT", "8545"))
        w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{anvil_port}"))
        for _ in range(30):
            try:
                if w3.is_connected():
                    break
            except Exception:
                pass
            time.sleep(0.5)
        assert w3.is_connected(), "Anvil not reachable"
        logger.info("Anvil Base fork connected, block=%d", w3.eth.block_number)

        # 2. Set up Safe ownership
        logger.info("=" * 60)
        logger.info("STEP 2: Setting up Safe ownership")
        logger.info("=" * 60)

        add_eoa_as_safe_owner(w3, SAFE_ADDRESS, EOA_WALLET)

        # 3. Drain pre-existing tokens from Safe
        logger.info("=" * 60)
        logger.info("STEP 3: Draining pre-existing tokens from Safe")
        logger.info("=" * 60)

        drain_safe_tokens(w3, SAFE_ADDRESS, [USDC_TOKEN, ALMANAK_TOKEN])

        # 4. Fund wallets
        logger.info("=" * 60)
        logger.info("STEP 4: Funding wallets")
        logger.info("=" * 60)

        # Give EOA and Safe plenty of ETH
        anvil_set_balance(w3, EOA_WALLET, 100 * 10**18)
        anvil_set_balance(w3, SAFE_ADDRESS, 10 * 10**18)

        # Read token decimals
        almanak_decimals = erc20_decimals(w3, ALMANAK_TOKEN)
        usdc_decimals = erc20_decimals(w3, USDC_TOKEN)
        logger.info("ALMANAK decimals=%d, USDC decimals=%d", almanak_decimals, usdc_decimals)

        # Fund Safe with tokens from the pool (which holds both)
        # ALMANAK: 2000 tokens (need ~1500 for LP + buffer)
        almanak_amount = 2000 * (10 ** almanak_decimals)
        pool_almanak_bal = erc20_balance(w3, ALMANAK_TOKEN, POOL_ADDRESS)
        logger.info("Pool ALMANAK balance: %d (need %d)", pool_almanak_bal, almanak_amount)

        if pool_almanak_bal >= almanak_amount:
            erc20_transfer(w3, ALMANAK_TOKEN, POOL_ADDRESS, SAFE_ADDRESS, almanak_amount)
        else:
            logger.error("Pool doesn't have enough ALMANAK! Has %d, need %d", pool_almanak_bal, almanak_amount)
            return False

        # USDC: 10 USDC (need ~2 for LP + buffer for Safe test tx)
        usdc_amount = 10 * (10 ** usdc_decimals)
        pool_usdc_bal = erc20_balance(w3, USDC_TOKEN, POOL_ADDRESS)
        logger.info("Pool USDC balance: %d (need %d)", pool_usdc_bal, usdc_amount)

        if pool_usdc_bal >= usdc_amount:
            erc20_transfer(w3, USDC_TOKEN, POOL_ADDRESS, SAFE_ADDRESS, usdc_amount)
        else:
            # Try Coinbase Commerce as USDC whale on Base
            coinbase_usdc = "0x3304E22DDaa22bCdC5fCa2269b418046aE7b566A"
            whale_bal = erc20_balance(w3, USDC_TOKEN, coinbase_usdc)
            if whale_bal >= usdc_amount:
                erc20_transfer(w3, USDC_TOKEN, coinbase_usdc, SAFE_ADDRESS, usdc_amount)
            else:
                logger.error("Cannot find USDC whale with enough balance")
                return False

        # Verify Safe balances
        safe_almanak = erc20_balance(w3, ALMANAK_TOKEN, SAFE_ADDRESS)
        safe_usdc = erc20_balance(w3, USDC_TOKEN, SAFE_ADDRESS)
        logger.info(
            "Safe funded: ALMANAK=%d (%d tokens), USDC=%d (%d tokens)",
            safe_almanak, safe_almanak // (10 ** almanak_decimals),
            safe_usdc, safe_usdc // (10 ** usdc_decimals),
        )

        # 4b. Diagnostic: check Safe configuration
        logger.info("=" * 60)
        logger.info("STEP 4b: Safe diagnostics")
        logger.info("=" * 60)

        safe_addr = w3.to_checksum_address(SAFE_ADDRESS)

        # Check Safe version
        version_abi = [{"inputs": [], "name": "VERSION", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"}]
        try:
            version_contract = w3.eth.contract(address=safe_addr, abi=version_abi)
            version = version_contract.functions.VERSION().call()
            logger.info("Safe version: %s", version)
        except Exception as e:
            logger.warning("Could not read Safe VERSION: %s", e)

        # Check Safe nonce
        nonce_abi = [{"inputs": [], "name": "nonce", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"}]
        nonce_contract = w3.eth.contract(address=safe_addr, abi=nonce_abi)
        safe_nonce = nonce_contract.functions.nonce().call()
        logger.info("Safe nonce: %d", safe_nonce)

        # Check if Safe has a guard configured
        guard_slot = "0x4a204f620c8c5ccdca3fd54d003badd85ba500436a431f0cbda4f558c93c34c8"
        guard_data = w3.eth.get_storage_at(safe_addr, guard_slot)
        guard_address = "0x" + guard_data[-20:].hex()
        logger.info("Safe guard address: %s", guard_address)
        if guard_address != "0x" + "00" * 20:
            logger.warning("Safe has a GUARD configured! This may block transactions.")

        # Check Safe threshold
        threshold_abi = [{"inputs": [], "name": "getThreshold", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"}]
        threshold_contract = w3.eth.contract(address=safe_addr, abi=threshold_abi)
        threshold = threshold_contract.functions.getThreshold().call()
        logger.info("Safe threshold: %d", threshold)

        # 4c. Diagnostic: try a SIMPLE transaction through the Safe
        logger.info("=" * 60)
        logger.info("STEP 4c: Test simple Safe transaction (approve USDC)")
        logger.info("=" * 60)

        from eth_account import Account
        from eth_keys import keys

        pk = os.environ.get(
            "ALMANAK_PRIVATE_KEY",
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        account = Account.from_key(pk)

        # Build a simple transfer(burnAddress, 1) call
        usdc_contract = w3.eth.contract(address=w3.to_checksum_address(USDC_TOKEN), abi=ERC20_ABI)
        # Build calldata by extracting from a built transaction
        dummy_tx = usdc_contract.functions.transfer(BURN_ADDRESS, 1).build_transaction({
            "from": safe_addr, "gas": 100_000, "gasPrice": 0, "nonce": 0,
        })
        approve_data = dummy_tx["data"]

        # Get Safe tx hash
        safe_tx_hash_abi = json.loads("""[{
            "inputs":[
                {"name":"to","type":"address"},{"name":"value","type":"uint256"},
                {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
                {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
                {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
                {"name":"refundReceiver","type":"address"},{"name":"_nonce","type":"uint256"}
            ],
            "name":"getTransactionHash","outputs":[{"type":"bytes32"}],
            "stateMutability":"view","type":"function"
        }]""")
        exec_tx_abi = json.loads("""[{
            "inputs":[
                {"name":"to","type":"address"},{"name":"value","type":"uint256"},
                {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
                {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
                {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
                {"name":"refundReceiver","type":"address"},{"name":"signatures","type":"bytes"}
            ],
            "name":"execTransaction","outputs":[{"name":"success","type":"bool"}],
            "stateMutability":"payable","type":"function"
        }]""")

        hash_contract = w3.eth.contract(address=safe_addr, abi=safe_tx_hash_abi)
        exec_contract = w3.eth.contract(address=safe_addr, abi=exec_tx_abi)

        zero_addr = "0x0000000000000000000000000000000000000000"
        usdc_addr = w3.to_checksum_address(USDC_TOKEN)

        safe_tx_hash = hash_contract.functions.getTransactionHash(
            usdc_addr, 0, bytes.fromhex(approve_data[2:]),
            0,  # CALL
            0, 0, 0,
            zero_addr, zero_addr,
            safe_nonce,
        ).call()

        # Sign the hash (raw, no EIP-191 prefix)
        private_key_bytes = account.key
        pk_obj = keys.PrivateKey(private_key_bytes)
        sig = pk_obj.sign_msg_hash(safe_tx_hash)
        r = sig.r.to_bytes(32, byteorder="big")
        s = sig.s.to_bytes(32, byteorder="big")
        v = sig.v
        if v < 27:
            v += 27
        signature = r + s + v.to_bytes(1, byteorder="big")

        # Try eth_call first to simulate
        try:
            call_tx = exec_contract.functions.execTransaction(
                usdc_addr, 0, bytes.fromhex(approve_data[2:]),
                0, 0, 0, 0, zero_addr, zero_addr, signature,
            ).build_transaction({
                "from": w3.to_checksum_address(EOA_WALLET),
                "gas": 5_000_000, "gasPrice": 0, "nonce": 0,
            })
            result = w3.eth.call({
                "from": w3.to_checksum_address(EOA_WALLET),
                "to": safe_addr,
                "data": call_tx["data"],
                "gas": 5_000_000,
            })
            logger.info("Simple Safe tx eth_call succeeded: %s", result.hex())
        except Exception as e:
            logger.error("Simple Safe tx eth_call FAILED: %s", e)

        # Actually execute it
        try:
            tx = exec_contract.functions.execTransaction(
                usdc_addr, 0, bytes.fromhex(approve_data[2:]),
                0, 0, 0, 0, zero_addr, zero_addr, signature,
            ).build_transaction({
                "from": w3.to_checksum_address(EOA_WALLET),
                "gas": 5_000_000,
                "gasPrice": w3.eth.gas_price,
                "nonce": w3.eth.get_transaction_count(w3.to_checksum_address(EOA_WALLET)),
            })
            tx_hash = w3.eth.send_transaction(tx)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            logger.info(
                "Simple Safe tx: status=%d, gasUsed=%d, tx_hash=%s",
                receipt["status"], receipt["gasUsed"], tx_hash.hex(),
            )
            if receipt["status"] == 1:
                logger.info("PASS: Simple Safe transaction works!")
            else:
                logger.error("FAIL: Simple Safe transaction reverted!")
                return False
        except Exception as e:
            logger.error("Simple Safe tx execution FAILED: %s", e)
            return False

        # 4d. Diagnostic: try a manual MultiSend through Safe
        logger.info("=" * 60)
        logger.info("STEP 4d: Test manual MultiSend via Safe (DELEGATECALL)")
        logger.info("=" * 60)

        # Build a simple MultiSend with 2 approvals
        from eth_abi import encode as abi_encode

        almanak_contract = w3.eth.contract(address=w3.to_checksum_address(ALMANAK_TOKEN), abi=ERC20_ABI)
        usdc_contract2 = w3.eth.contract(address=w3.to_checksum_address(USDC_TOKEN), abi=ERC20_ABI)

        # Build inner tx calldata (approve 1 wei to burn address)
        tx1_data_dict = almanak_contract.functions.transfer(BURN_ADDRESS, 1).build_transaction({
            "from": safe_addr, "gas": 100_000, "gasPrice": 0, "nonce": 0,
        })
        tx1_data = bytes.fromhex(tx1_data_dict["data"][2:])

        tx2_data_dict = usdc_contract2.functions.transfer(BURN_ADDRESS, 1).build_transaction({
            "from": safe_addr, "gas": 100_000, "gasPrice": 0, "nonce": 0,
        })
        tx2_data = bytes.fromhex(tx2_data_dict["data"][2:])

        # Pack in MultiSend format: operation(1) + to(20) + value(32) + dataLen(32) + data
        almanak_addr_bytes = bytes.fromhex(ALMANAK_TOKEN[2:])
        usdc_addr_bytes = bytes.fromhex(USDC_TOKEN[2:])

        packed = (
            # Tx 1: CALL to ALMANAK token
            (0).to_bytes(1, "big") + almanak_addr_bytes +
            (0).to_bytes(32, "big") + len(tx1_data).to_bytes(32, "big") + tx1_data +
            # Tx 2: CALL to USDC token
            (0).to_bytes(1, "big") + usdc_addr_bytes +
            (0).to_bytes(32, "big") + len(tx2_data).to_bytes(32, "big") + tx2_data
        )

        # Encode as multiSend(bytes)
        encoded_bytes = abi_encode(["bytes"], [packed])
        multisend_selector = bytes.fromhex("8d80ff0a")
        multisend_calldata = multisend_selector + encoded_bytes

        multisend_addr = w3.to_checksum_address("0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526")

        # Verify MultiSend contract exists
        ms_code = w3.eth.get_code(multisend_addr)
        logger.info("MultiSend contract code length: %d bytes", len(ms_code))
        if len(ms_code) < 10:
            logger.error("MultiSend contract not deployed at %s!", multisend_addr)
            return False

        # Read updated Safe nonce
        safe_nonce_now = nonce_contract.functions.nonce().call()
        logger.info("Current Safe nonce: %d", safe_nonce_now)

        # Compute Safe tx hash for DELEGATECALL to MultiSend
        safe_tx_hash_ms = hash_contract.functions.getTransactionHash(
            multisend_addr, 0, multisend_calldata,
            1,  # DELEGATECALL
            0, 0, 0,
            zero_addr, zero_addr,
            safe_nonce_now,
        ).call()

        logger.info("Safe tx hash for MultiSend: %s", safe_tx_hash_ms.hex())

        # Sign
        sig_ms = pk_obj.sign_msg_hash(safe_tx_hash_ms)
        r_ms = sig_ms.r.to_bytes(32, byteorder="big")
        s_ms = sig_ms.s.to_bytes(32, byteorder="big")
        v_ms = sig_ms.v
        if v_ms < 27:
            v_ms += 27
        signature_ms = r_ms + s_ms + v_ms.to_bytes(1, byteorder="big")

        # Try eth_call first
        try:
            ms_call_tx = exec_contract.functions.execTransaction(
                multisend_addr, 0, multisend_calldata,
                1,  # DELEGATECALL
                0, 0, 0, zero_addr, zero_addr, signature_ms,
            ).build_transaction({
                "from": w3.to_checksum_address(EOA_WALLET),
                "gas": 5_000_000, "gasPrice": 0, "nonce": 0,
            })
            result_ms = w3.eth.call({
                "from": w3.to_checksum_address(EOA_WALLET),
                "to": safe_addr,
                "data": ms_call_tx["data"],
                "gas": 5_000_000,
            })
            logger.info("Manual MultiSend eth_call succeeded: %s", result_ms.hex())
        except Exception as e:
            logger.error("Manual MultiSend eth_call FAILED: %s", e)
            # Try without DELEGATECALL (just CALL) to see if it's a DELEGATECALL issue
            logger.info("Trying same MultiSend as CALL instead of DELEGATECALL...")
            safe_tx_hash_call = hash_contract.functions.getTransactionHash(
                multisend_addr, 0, multisend_calldata,
                0,  # CALL
                0, 0, 0, zero_addr, zero_addr,
                safe_nonce_now,
            ).call()
            sig_call = pk_obj.sign_msg_hash(safe_tx_hash_call)
            r_c = sig_call.r.to_bytes(32, byteorder="big")
            s_c = sig_call.s.to_bytes(32, byteorder="big")
            v_c = sig_call.v if sig_call.v >= 27 else sig_call.v + 27
            signature_call = r_c + s_c + v_c.to_bytes(1, byteorder="big")
            try:
                call_tx2 = exec_contract.functions.execTransaction(
                    multisend_addr, 0, multisend_calldata,
                    0,  # CALL
                    0, 0, 0, zero_addr, zero_addr, signature_call,
                ).build_transaction({
                    "from": w3.to_checksum_address(EOA_WALLET),
                    "gas": 5_000_000, "gasPrice": 0, "nonce": 0,
                })
                result_call = w3.eth.call({
                    "from": w3.to_checksum_address(EOA_WALLET),
                    "to": safe_addr,
                    "data": call_tx2["data"],
                    "gas": 5_000_000,
                })
                logger.info("MultiSend as CALL eth_call succeeded: %s", result_call.hex())
                logger.warning("CALL works but DELEGATECALL fails - issue with MultiSend DELEGATECALL")
            except Exception as e2:
                logger.error("MultiSend as CALL also FAILED: %s", e2)

        # 5. Connect gateway client + compile LP intent
        logger.info("=" * 60)
        logger.info("STEP 5: Compile LP intent and diagnose inner transactions")
        logger.info("=" * 60)

        from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

        gw_config = GatewayClientConfig(host="localhost", port=gw_port)
        gw_client = GatewayClient(gw_config)
        gw_client.connect()

        if not gw_client.wait_for_ready(timeout=15.0):
            logger.error("Gateway not ready")
            return False

        # 5a. Compile the LP_OPEN intent via gateway (same as executor would)
        logger.info("--- Step 5a: Compiling LP_OPEN intent via gateway ---")

        from almanak.gateway.proto import gateway_pb2

        # Compiler expects range_lower/range_upper in "token1 per token0" units.
        # token0 = USDC (0x8335...), token1 = ALMANAK (0xdefa...) after address sort.
        # So prices are "ALMANAK per USDC".
        # Current pool price ≈ 499 ALMANAK/USDC (tick 338461).
        # Range 200-1000 ALMANAK/USDC brackets the current price.
        #
        # Amounts must be balanced for the pool ratio to pass the 20% LP slippage
        # check. At ~499 ALMANAK/USDC in range [200,1000], the liquidity ratio is
        # roughly 625 ALMANAK per 1 USDC. Using 2 USDC + 1500 ALMANAK (20% excess
        # on ALMANAK side) ensures both amount0Min and amount1Min pass.
        intent_data = json.dumps({
            "pool": f"{USDC_TOKEN}/{ALMANAK_TOKEN}/3000",
            "amount0": "2",       # USDC (token0) - limiting side
            "amount1": "1500",    # ALMANAK (token1) - ~20% excess over ideal ~1250
            "protocol": "uniswap_v3",
            "range_lower": "200",
            "range_upper": "1000",
        }).encode()

        compile_resp = gw_client.execution.CompileIntent(
            gateway_pb2.CompileIntentRequest(
                intent_type="lp_open",
                intent_data=intent_data,
                chain="base",
                wallet_address=SAFE_ADDRESS,
            )
        )

        if not compile_resp.success:
            logger.error("Compilation FAILED: %s", compile_resp.error)
            return False

        # Decode the compiled action bundle
        bundle_data = json.loads(compile_resp.action_bundle.decode("utf-8"))
        transactions = bundle_data.get("transactions", [])
        metadata = bundle_data.get("metadata", {})

        logger.info("Compiled LP bundle: %d transactions", len(transactions))
        logger.info("Bundle metadata: tick_lower=%s, tick_upper=%s", metadata.get("tick_lower"), metadata.get("tick_upper"))
        logger.info("Bundle metadata: amount0_desired=%s, amount1_desired=%s", metadata.get("amount0_desired"), metadata.get("amount1_desired"))
        logger.info("Bundle metadata: position_manager=%s", metadata.get("position_manager"))

        for i, tx in enumerate(transactions):
            data_hex = tx.get("data", "0x")
            logger.info(
                "  TX[%d]: to=%s, value=%s, data_len=%d, type=%s, desc=%s",
                i, tx.get("to", "?")[:16], tx.get("value", 0),
                len(data_hex) // 2 if data_hex else 0,
                tx.get("tx_type", "?"),
                tx.get("description", "?")[:80],
            )

        # 5b. Diagnostic: test inner transactions via eth_call (snapshot/revert to keep state clean)
        logger.info("--- Step 5b: Testing inner TXs via eth_call from Safe (snapshot) ---")

        # Snapshot so diagnostic steps don't affect the real execution in step 6
        snapshot_id = w3.provider.make_request("evm_snapshot", [])["result"]
        logger.info("Anvil snapshot created: %s", snapshot_id)

        anvil_impersonate(w3, SAFE_ADDRESS)
        all_inner_pass = True
        for i, tx in enumerate(transactions):
            tx_to = w3.to_checksum_address(tx["to"])
            tx_data = tx.get("data", "0x")
            tx_value = int(tx.get("value", 0))

            try:
                result_bytes = w3.eth.call({
                    "from": w3.to_checksum_address(SAFE_ADDRESS),
                    "to": tx_to,
                    "data": tx_data,
                    "value": tx_value,
                    "gas": 5_000_000,
                })
                logger.info("  TX[%d] eth_call from Safe: SUCCESS (result: %s)", i, result_bytes.hex()[:40])
            except Exception as e:
                logger.error("  TX[%d] eth_call from Safe: FAILED: %s", i, e)
                all_inner_pass = False

        # Execute approve(s) on-chain, then test mint via eth_call
        if len(transactions) >= 2:
            logger.info("--- Step 5b2: Execute approve(s) from Safe, then test mint ---")
            for i, tx in enumerate(transactions[:-1]):
                tx_to = w3.to_checksum_address(tx["to"])
                tx_data = tx.get("data", "0x")
                try:
                    send_tx = {
                        "from": w3.to_checksum_address(SAFE_ADDRESS),
                        "to": tx_to,
                        "data": tx_data,
                        "value": int(tx.get("value", 0)),
                        "gas": 500_000,
                        "gasPrice": w3.eth.gas_price,
                        "nonce": w3.eth.get_transaction_count(w3.to_checksum_address(SAFE_ADDRESS)),
                    }
                    tx_hash = w3.eth.send_transaction(send_tx)
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                    logger.info("  TX[%d] (approve) executed: status=%d", i, receipt["status"])
                except Exception as e:
                    logger.error("  TX[%d] (approve) execution failed: %s", i, e)

            # Now test the mint via eth_call (approvals are committed)
            mint_tx = transactions[-1]
            try:
                result_bytes = w3.eth.call({
                    "from": w3.to_checksum_address(SAFE_ADDRESS),
                    "to": w3.to_checksum_address(mint_tx["to"]),
                    "data": mint_tx.get("data", "0x"),
                    "value": int(mint_tx.get("value", 0)),
                    "gas": 5_000_000,
                })
                logger.info("  Mint eth_call (after approve): SUCCESS (result: %s)", result_bytes.hex()[:80])
            except Exception as e:
                logger.error("  Mint eth_call (after approve): FAILED: %s", e)
                all_inner_pass = False

        anvil_stop_impersonate(w3, SAFE_ADDRESS)

        if not all_inner_pass:
            logger.error("=" * 60)
            logger.error("Inner LP transactions REVERT - this is the real bug, not GS013!")
            logger.error("Fix the inner transactions before testing Safe MultiSend.")
            logger.error("=" * 60)

        # Revert snapshot to restore clean state for step 6
        w3.provider.make_request("evm_revert", [snapshot_id])
        logger.info("Anvil snapshot reverted (clean state for step 6)")

        # 5c. Test the MultiSend bundle via eth_call from Safe (bypassing execTransaction)
        logger.info("--- Step 5c: Testing MultiSend bundle via Safe.execTransaction ---")

        # Build MultiSend from the compiled transactions
        from almanak.framework.execution.interfaces import UnsignedTransaction, TransactionType
        from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder

        unsigned_txs = []
        for tx in transactions:
            unsigned_txs.append(UnsignedTransaction(
                to=tx["to"],
                value=int(tx.get("value", 0)),
                data=tx.get("data", "0x"),
                chain_id=8453,  # Base
                gas_limit=int(tx.get("gas_estimate", 300_000)),
                tx_type=TransactionType.EIP_1559,
                max_fee_per_gas=100_000_000,  # Placeholder
                max_priority_fee_per_gas=1_000_000,
            ))

        multisend_calldata = MultiSendEncoder.encode_transactions(unsigned_txs, w3)
        multisend_addr = w3.to_checksum_address("0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526")
        multisend_data = "0x" + multisend_calldata.hex()

        logger.info("MultiSend calldata length: %d bytes", len(multisend_calldata))

        # Decode and verify the MultiSend data
        decoded_txs = MultiSendEncoder.decode_multisend_data(multisend_calldata)
        for i, dtx in enumerate(decoded_txs):
            logger.info(
                "  Decoded TX[%d]: op=%d, to=%s, value=%s, data_len=%d",
                i, dtx["operation"], dtx["to"][:16], dtx["value"], len(dtx["data"]) // 2,
            )

        # Read Safe nonce (snapshot reverted, so this is after step 4c/4d only)
        safe_nonce_now2 = nonce_contract.functions.nonce().call()
        logger.info("Current Safe nonce for MultiSend test: %d", safe_nonce_now2)

        # Build execTransaction with safeTxGas=0 and gasPrice=0 (standard)
        safe_tx_hash_lp = hash_contract.functions.getTransactionHash(
            multisend_addr, 0, multisend_data,
            1,  # DELEGATECALL
            0, 0, 0,
            zero_addr, zero_addr,
            safe_nonce_now2,
        ).call()

        sig_lp = pk_obj.sign_msg_hash(safe_tx_hash_lp)
        r_lp = sig_lp.r.to_bytes(32, byteorder="big")
        s_lp = sig_lp.s.to_bytes(32, byteorder="big")
        v_lp = sig_lp.v if sig_lp.v >= 27 else sig_lp.v + 27
        signature_lp = r_lp + s_lp + v_lp.to_bytes(1, byteorder="big")

        # Try eth_call first
        try:
            lp_call_tx = exec_contract.functions.execTransaction(
                multisend_addr, 0, multisend_data,
                1,  # DELEGATECALL
                0, 0, 0, zero_addr, zero_addr, signature_lp,
            ).build_transaction({
                "from": w3.to_checksum_address(EOA_WALLET),
                "gas": 10_000_000, "gasPrice": 0, "nonce": 0,
            })
            result_lp = w3.eth.call({
                "from": w3.to_checksum_address(EOA_WALLET),
                "to": safe_addr,
                "data": lp_call_tx["data"],
                "gas": 10_000_000,
            })
            logger.info("LP MultiSend via Safe eth_call: SUCCESS: %s", result_lp.hex())
        except Exception as e:
            logger.error("LP MultiSend via Safe eth_call: FAILED: %s", e)
            # Try with safeTxGas=1 to avoid GS013 masking
            logger.info("Retrying with safeTxGas=1 to get real revert reason...")
            safe_tx_hash_lp2 = hash_contract.functions.getTransactionHash(
                multisend_addr, 0, multisend_data,
                1,  # DELEGATECALL
                1,  # safeTxGas=1 (non-zero so Safe returns false instead of GS013)
                0, 0,
                zero_addr, zero_addr,
                safe_nonce_now2,
            ).call()
            sig_lp2 = pk_obj.sign_msg_hash(safe_tx_hash_lp2)
            r_lp2 = sig_lp2.r.to_bytes(32, byteorder="big")
            s_lp2 = sig_lp2.s.to_bytes(32, byteorder="big")
            v_lp2 = sig_lp2.v if sig_lp2.v >= 27 else sig_lp2.v + 27
            signature_lp2 = r_lp2 + s_lp2 + v_lp2.to_bytes(1, byteorder="big")
            try:
                lp_call_tx2 = exec_contract.functions.execTransaction(
                    multisend_addr, 0, multisend_data,
                    1, 1, 0, 0, zero_addr, zero_addr, signature_lp2,
                ).build_transaction({
                    "from": w3.to_checksum_address(EOA_WALLET),
                    "gas": 10_000_000, "gasPrice": 0, "nonce": 0,
                })
                result_lp2 = w3.eth.call({
                    "from": w3.to_checksum_address(EOA_WALLET),
                    "to": safe_addr,
                    "data": lp_call_tx2["data"],
                    "gas": 10_000_000,
                })
                # With safeTxGas=1, returns bool (true/false) instead of reverting
                logger.info("With safeTxGas=1: result=%s (0x01=success, 0x00=inner revert)", result_lp2.hex())
            except Exception as e2:
                logger.error("With safeTxGas=1 also failed: %s", e2)

        # 6. Full execution via ToolExecutor (the original test)
        logger.info("=" * 60)
        logger.info("STEP 6: Full LP open via ToolExecutor + Safe MultiSend")
        logger.info("=" * 60)

        from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog

        policy = AgentPolicy(
            allowed_chains={"base"},
            allowed_tokens={"ALMANAK", "USDC", "ETH"},
            allowed_execution_wallets={EOA_WALLET, SAFE_ADDRESS},
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            cooldown_seconds=0,
            max_trades_per_hour=100,
            require_rebalance_check=False,
            allowed_tools={
                "get_price", "get_balance", "get_pool_state", "get_lp_position",
                "get_indicator", "resolve_token", "compute_rebalance_candidate",
                "simulate_intent", "open_lp_position", "close_lp_position",
                "swap_tokens", "save_agent_state", "load_agent_state",
                "record_agent_decision",
            },
        )

        catalog = get_default_catalog()
        executor = ToolExecutor(
            gw_client,
            policy=policy,
            catalog=catalog,
            wallet_address=EOA_WALLET,
            strategy_id="e2e-safe-lp-test",
            default_chain="base",
        )

        try:
            # The executor expects price_lower/price_upper as "token_b per token_a".
            # token_a = ALMANAK, token_b = USDC → prices are "USDC per ALMANAK".
            # ALMANAK ≈ $0.002, so 1 ALMANAK ≈ 0.002 USDC.
            # Range 0.001-0.005 USDC/ALMANAK.
            # Executor inverts (because ALMANAK addr > USDC addr):
            #   1/0.005 = 200, 1/0.001 = 1000 → 200-1000 ALMANAK/USDC
            # This brackets the current pool price ~499 ALMANAK/USDC (tick 338461).
            #
            # Amounts: 1500 ALMANAK + 2 USDC (balanced for pool ratio ~625:1).
            # After executor swap: amount0=2 USDC, amount1=1500 ALMANAK.
            result = await executor.execute("open_lp_position", {
                "token_a": ALMANAK_TOKEN,
                "token_b": USDC_TOKEN,
                "amount_a": "1500",
                "amount_b": "2",
                "price_lower": "0.001",
                "price_upper": "0.005",
                "fee_tier": 3000,
                "protocol": "uniswap_v3",
                "chain": "base",
                "execution_wallet": SAFE_ADDRESS,
            })

            logger.info("LP open result: status=%s", result.status)
            logger.info("LP open data: %s", json.dumps(result.model_dump(exclude_none=True), indent=2, default=str))

            if result.status == "success":
                logger.info("=" * 60)
                logger.info("SUCCESS: LP position opened via Safe MultiSend!")
                logger.info("=" * 60)
                return True
            else:
                logger.error("FAILED: LP open status=%s", result.status)
                return False
        except Exception as e:
            logger.error("LP open raised exception: %s", e)
            return False

    except Exception as e:
        logger.exception("Test failed with exception: %s", e)
        return False

    finally:
        try:
            gw_client.disconnect()
        except Exception:
            pass
        gateway.stop()
        logger.info("Gateway stopped")


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
