"""Intent tests for Ethena complete_unstake path (VIB-1529).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Ethena's two-phase unstaking:

Phase 1: UnstakeIntent -> cooldownAssets(sUSDe) -- starts 7-day cooldown
Phase 2: complete_unstake -> unstake(address) -- withdraws USDe after cooldown

Background:
    iter 99 caught a P0 selector bug: ETHENA_UNSTAKE_SELECTOR was 0x2e17de78
    (unstake(uint256)) instead of 0xf2888dbb (unstake(address)). The wrong
    selector caused all complete_unstake calls to revert. PR #845 was closed
    without merging so this test file is the definitive fix + regression guard.

    Selector derivation:
        cast sig "unstake(address)" == 0xf2888dbb  <- CORRECT
        cast sig "unstake(uint256)" == 0x2e17de78  <- WRONG (previous value)

Test layers:
    1. Compilation: Verify calldata uses the correct selector
    2. Execution:   On Anvil fork, execute cooldown + time-warp + complete_unstake
    3. Receipt:     Parse Withdraw event, assert USDe returned > 0
    4. Balances:    Assert USDe balance increased after complete_unstake

To run (requires Ethereum Anvil fork on port 8545):
    uv run pytest tests/intents/ethereum/test_ethena_unstake_complete.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.ethena.adapter import (
    ETHENA_ADDRESSES,
    ETHENA_COOLDOWN_ASSETS_SELECTOR,
    ETHENA_DEPOSIT_SELECTOR,
    ETHENA_UNSTAKE_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    EthenaAdapter,
    EthenaConfig,
)
from almanak.framework.connectors.ethena.receipt_parser import EthenaReceiptParser
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import UnstakeIntent

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"
TEST_WALLET = "0x1234567890123456789012345678901234567890"
SUSDE_ADDRESS = ETHENA_ADDRESSES["ethereum"]["susde"]
USDE_ADDRESS = ETHENA_ADDRESSES["ethereum"]["usde"]

# Ethena cooldown period: 7 days in seconds
COOLDOWN_SECONDS = 7 * 24 * 3600  # 604800

# USDe ERC-20 balance storage slot (standard OZ ERC-20 layout: _balances at slot 0)
USDE_BALANCE_SLOT = 0
SUSDE_BALANCE_SLOT = 0

ANVIL_URL = "http://localhost:8545"

# =============================================================================
# Layer 1: Selector Correctness Tests (No Anvil Required)
# =============================================================================


class TestEthenaUnstakeSelectorCorrectness:
    """Verify the ETHENA_UNSTAKE_SELECTOR is the correct 4-byte selector.

    This is a regression guard for the P0 bug caught in iter 99:
    the selector was 0x2e17de78 (unstake(uint256)) instead of
    0xf2888dbb (unstake(address)).
    """

    def test_unstake_selector_is_address_variant(self):
        """ETHENA_UNSTAKE_SELECTOR must be for unstake(address), not unstake(uint256).

        Derived from: keccak256("unstake(address)")[:4] = 0xf2888dbb
        """
        assert ETHENA_UNSTAKE_SELECTOR == "0xf2888dbb", (
            f"Wrong selector! Got {ETHENA_UNSTAKE_SELECTOR}, "
            "expected 0xf2888dbb (unstake(address)). "
            "0x2e17de78 is unstake(uint256) -- the wrong function."
        )

    def test_wrong_selector_not_used(self):
        """The old buggy selector 0x2e17de78 (unstake(uint256)) must NOT be used."""
        assert ETHENA_UNSTAKE_SELECTOR != "0x2e17de78", (
            "ETHENA_UNSTAKE_SELECTOR is still 0x2e17de78 (unstake(uint256)) -- "
            "this is the bug from iter 99 / VIB-1529. Fix: use 0xf2888dbb (unstake(address))."
        )

    def test_selector_format(self):
        """Selector must be a 4-byte hex string (0x + 8 hex chars)."""
        assert ETHENA_UNSTAKE_SELECTOR.startswith("0x")
        assert len(ETHENA_UNSTAKE_SELECTOR) == 10  # 0x + 8 hex chars = 10 total

    def test_selector_derivation_with_web3(self):
        """Cross-check selector derivation using web3's keccak implementation."""
        expected = "0x" + Web3.keccak(text="unstake(address)").hex()[:8]
        assert ETHENA_UNSTAKE_SELECTOR == expected, (
            f"Selector mismatch: adapter has {ETHENA_UNSTAKE_SELECTOR}, "
            f"keccak256('unstake(address)') = {expected}"
        )

    def test_cooldown_selector_is_correct(self):
        """ETHENA_COOLDOWN_ASSETS_SELECTOR must be for cooldownAssets(uint256)."""
        expected = "0x" + Web3.keccak(text="cooldownAssets(uint256)").hex()[:8]
        assert ETHENA_COOLDOWN_ASSETS_SELECTOR == expected, (
            f"cooldownAssets selector mismatch: got {ETHENA_COOLDOWN_ASSETS_SELECTOR}, "
            f"expected {expected}"
        )

    def test_deposit_selector_is_correct(self):
        """ETHENA_DEPOSIT_SELECTOR must be for deposit(uint256,address)."""
        expected = "0x" + Web3.keccak(text="deposit(uint256,address)").hex()[:8]
        assert ETHENA_DEPOSIT_SELECTOR == expected, (
            f"deposit selector mismatch: got {ETHENA_DEPOSIT_SELECTOR}, expected {expected}"
        )

    def test_approve_selector_is_correct(self):
        """ERC20_APPROVE_SELECTOR must be for approve(address,uint256)."""
        expected = "0x" + Web3.keccak(text="approve(address,uint256)").hex()[:8]
        assert ERC20_APPROVE_SELECTOR == expected, (
            f"approve selector mismatch: got {ERC20_APPROVE_SELECTOR}, expected {expected}"
        )


class TestEthenaCompleteUnstakeCalldata:
    """Verify complete_unstake() builds correct calldata with the fixed selector."""

    def _make_adapter(self) -> EthenaAdapter:
        return EthenaAdapter(EthenaConfig(chain=CHAIN_NAME, wallet_address=TEST_WALLET))

    def test_complete_unstake_calldata_uses_correct_selector(self):
        """complete_unstake() calldata must start with 0xf2888dbb (unstake(address))."""
        adapter = self._make_adapter()
        result = adapter.complete_unstake(receiver=TEST_WALLET)

        assert result.success, f"complete_unstake build failed: {result.error}"
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        assert calldata.startswith("0xf2888dbb"), (
            f"Wrong selector in calldata! Got prefix: {calldata[:10]}. "
            "Expected 0xf2888dbb (unstake(address))."
        )

    def test_complete_unstake_calldata_encodes_receiver(self):
        """complete_unstake() calldata must ABI-encode the receiver address."""
        adapter = self._make_adapter()
        # Use a distinct address to verify it's encoded
        receiver = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF"
        result = adapter.complete_unstake(receiver=receiver)

        assert result.success
        calldata = result.tx_data["data"]
        # ABI-encoded address: selector (4 bytes) + zero-padded address (32 bytes)
        # Use [2:] (not lstrip) to safely strip "0x" prefix regardless of address content
        encoded_receiver = receiver.lower()[2:].rjust(64, "0")
        assert calldata.lower() == f"{ETHENA_UNSTAKE_SELECTOR.lower()}{encoded_receiver}", (
            f"Receiver address {receiver} was not ABI-encoded correctly in calldata: {calldata}"
        )

    def test_complete_unstake_default_receiver_is_wallet(self):
        """complete_unstake() with no receiver arg uses the wallet address."""
        adapter = self._make_adapter()
        result = adapter.complete_unstake()

        assert result.success
        calldata = result.tx_data["data"]
        # Use [2:] (not lstrip) to safely strip "0x" prefix
        encoded_wallet = TEST_WALLET.lower()[2:].rjust(64, "0")
        assert calldata.lower() == f"{ETHENA_UNSTAKE_SELECTOR.lower()}{encoded_wallet}"

    def test_complete_unstake_targets_susde_contract(self):
        """complete_unstake() transaction must target the sUSDe contract."""
        adapter = self._make_adapter()
        result = adapter.complete_unstake(receiver=TEST_WALLET)

        assert result.success
        assert result.tx_data["to"].lower() == SUSDE_ADDRESS.lower()


class TestEthenaUnstakeIntentCompilation:
    """Layer 1: IntentCompiler compilation for Ethena unstake (cooldown phase)."""

    def test_compile_unstake_intent_success(self):
        """UnstakeIntent compiles to cooldown transaction with correct target."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert len(result.transactions) == 1
        # The cooldown tx must target sUSDe contract
        tx = result.transactions[0]
        assert tx.to.lower() == SUSDE_ADDRESS.lower()

    def test_compile_unstake_cooldown_calldata_selector(self):
        """Compiled cooldown tx must use cooldownAssets selector."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("500.0"),
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        tx = result.transactions[0]
        assert tx.data.startswith(ETHENA_COOLDOWN_ASSETS_SELECTOR), (
            f"Expected cooldownAssets selector {ETHENA_COOLDOWN_ASSETS_SELECTOR}, "
            f"got calldata: {tx.data[:10]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Tests (Requires Anvil Ethereum Fork)
# =============================================================================


def _is_anvil_running(url: str = ANVIL_URL) -> bool:
    """Check if Anvil is accessible."""
    try:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": (2, 5)}))
        return w3.is_connected()
    except Exception:
        return False


def _fund_erc20_balance(
    rpc_url: str, wallet: str, token_address: str, amount: int, slot: int
) -> None:
    """Set ERC-20 token balance via anvil_setStorageAt."""
    import requests

    # Calculate storage slot: keccak256(abi.encode(wallet, slot))
    wallet_padded = wallet.lower().replace("0x", "").zfill(64)
    slot_padded = hex(slot)[2:].zfill(64)
    mapping_key = wallet_padded + slot_padded
    storage_slot = "0x" + Web3.keccak(hexstr=mapping_key).hex()
    amount_hex = "0x" + hex(amount)[2:].zfill(64)

    resp = requests.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "method": "anvil_setStorageAt",
            "params": [token_address, storage_slot, amount_hex],
            "id": 1,
        },
        timeout=10,
    )
    resp.raise_for_status()


@pytest.mark.ethereum
@pytest.mark.integration
@pytest.mark.skipif(not _is_anvil_running(), reason="Anvil not running (Ethereum fork required)")
class TestEthenaCompleteUnstakeOnAnvil:
    """Layers 2-4: Full on-chain Ethena complete_unstake tests.

    Requires Anvil running on port 8545 as an Ethereum mainnet fork.

    Test flow:
        1. Fund wallet with USDe (storage manipulation)
        2. Approve sUSDe contract to spend USDe
        3. Stake USDe -> receive sUSDe
        4. Initiate cooldown (cooldownAssets)
        5. Time-warp 7 days (evm_increaseTime + evm_mine)
        6. Execute complete_unstake (unstake(address))
        7. Assert USDe balance increased
    """

    @pytest.fixture(scope="class")
    def w3(self) -> Web3:
        w3 = Web3(Web3.HTTPProvider(ANVIL_URL, request_kwargs={"timeout": (3, 15)}))
        assert w3.is_connected(), "Anvil not reachable"
        assert w3.eth.chain_id == 1, f"Expected Ethereum (chainId 1), got {w3.eth.chain_id}"
        return w3

    @pytest.fixture(scope="class")
    def wallet_key(self) -> str:
        """Anvil default test private key."""
        return "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

    @pytest.fixture(scope="class")
    def wallet_address(self, wallet_key: str) -> str:
        from eth_account import Account
        return Account.from_key(wallet_key).address

    @pytest.fixture(scope="class")
    def funded_wallet(self, w3: Web3, wallet_address: str) -> str:
        """Fund test wallet with ETH and USDe."""
        import requests

        # Fund ETH
        requests.post(ANVIL_URL, json={
            "jsonrpc": "2.0", "method": "anvil_setBalance",
            "params": [wallet_address, "0x" + hex(10 * 10**18)[2:]],
            "id": 1,
        }, timeout=10)

        # Fund USDe (10,000 USDe = 10_000 * 10^18)
        usde_amount = 10_000 * 10**18
        _fund_erc20_balance(ANVIL_URL, wallet_address, USDE_ADDRESS, usde_amount, USDE_BALANCE_SLOT)

        # Mine a block to apply state changes
        requests.post(ANVIL_URL, json={
            "jsonrpc": "2.0", "method": "evm_mine", "params": [], "id": 2,
        }, timeout=10)

        return wallet_address

    @pytest.mark.asyncio
    async def test_complete_unstake_full_lifecycle(
        self,
        w3: Web3,
        funded_wallet: str,
        wallet_key: str,
    ):
        """Layer 2-4: Execute full Ethena stake -> cooldown -> time-warp -> complete_unstake.

        Flow:
            1. Record USDe balance before
            2. Approve + stake 1000 USDe -> sUSDe
            3. Initiate cooldown
            4. evm_increaseTime(604800) + evm_mine
            5. complete_unstake()
            6. Parse receipt -- assert Withdraw event present
            7. Assert USDe balance increased
        """
        from eth_account import Account
        from web3.middleware import SignAndSendRawMiddlewareBuilder

        account = Account.from_key(wallet_key)
        w3.middleware_onion.inject(SignAndSendRawMiddlewareBuilder.build(account), layer=0)
        w3.eth.default_account = funded_wallet

        adapter = EthenaAdapter(EthenaConfig(chain=CHAIN_NAME, wallet_address=funded_wallet))
        parser = EthenaReceiptParser()

        usde_contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDE_ADDRESS),
            abi=[
                {"name": "balanceOf", "type": "function", "inputs": [{"type": "address"}],
                 "outputs": [{"type": "uint256"}], "stateMutability": "view"},
                {"name": "approve", "type": "function",
                 "inputs": [{"type": "address"}, {"type": "uint256"}],
                 "outputs": [{"type": "bool"}], "stateMutability": "nonpayable"},
            ],
        )

        # --- Record USDe balance before ---
        usde_before = usde_contract.functions.balanceOf(funded_wallet).call()
        print(f"\nUSDe before: {usde_before / 10**18:.2f}")
        assert usde_before > 0, "Wallet has no USDe -- funding failed"

        # --- Layer 2: Execute stake (approve + deposit) ---
        stake_amount = Decimal("1000.0")
        stake_result = adapter.stake_usde(stake_amount)
        assert stake_result.success, f"stake_usde failed: {stake_result.error}"

        # Send approve TX
        approve_result = adapter.approve_usde(stake_amount)
        assert approve_result.success, f"approve_usde failed: {approve_result.error}"
        if approve_result.tx_data:
            tx_hash = w3.eth.send_transaction({
                "to": approve_result.tx_data["to"],
                "data": approve_result.tx_data["data"],
                "from": funded_wallet,
                "gas": approve_result.gas_estimate,
            })
            w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

        # Send stake TX
        if stake_result.tx_data:
            tx_hash = w3.eth.send_transaction({
                "to": stake_result.tx_data["to"],
                "data": stake_result.tx_data["data"],
                "from": funded_wallet,
                "gas": stake_result.gas_estimate,
            })
            stake_receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            assert stake_receipt["status"] == 1, "Stake TX failed"
            print(f"Stake TX: {tx_hash.hex()}")

        # --- Initiate cooldown ---
        cooldown_result = adapter.unstake_susde(stake_amount)
        assert cooldown_result.success, f"unstake_susde failed: {cooldown_result.error}"
        if cooldown_result.tx_data:
            tx_hash = w3.eth.send_transaction({
                "to": cooldown_result.tx_data["to"],
                "data": cooldown_result.tx_data["data"],
                "from": funded_wallet,
                "gas": cooldown_result.gas_estimate,
            })
            cooldown_receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            assert cooldown_receipt["status"] == 1, "Cooldown TX failed"
            print(f"Cooldown TX: {tx_hash.hex()}")

        # --- Layer 2: Time-warp past cooldown (7 days) ---
        import requests

        requests.post(ANVIL_URL, json={
            "jsonrpc": "2.0", "method": "evm_increaseTime",
            "params": [COOLDOWN_SECONDS + 60],  # 7 days + 60s buffer
            "id": 10,
        }, timeout=10)
        requests.post(ANVIL_URL, json={
            "jsonrpc": "2.0", "method": "evm_mine", "params": [], "id": 11,
        }, timeout=10)
        print(f"Time-warped {COOLDOWN_SECONDS} seconds (7 days)")

        # Capture balance immediately before complete_unstake (after cooldown consumed sUSDe)
        usde_before_complete = usde_contract.functions.balanceOf(funded_wallet).call()

        # --- Layer 2: Execute complete_unstake ---
        complete_result = adapter.complete_unstake(receiver=funded_wallet)
        assert complete_result.success, f"complete_unstake build failed: {complete_result.error}"
        assert complete_result.tx_data is not None

        # Verify calldata uses correct selector BEFORE sending
        calldata = complete_result.tx_data["data"]
        assert calldata.startswith("0xf2888dbb"), (
            f"Selector bug detected in complete_unstake calldata: {calldata[:10]}"
        )

        tx_hash = w3.eth.send_transaction({
            "to": complete_result.tx_data["to"],
            "data": calldata,
            "from": funded_wallet,
            "gas": complete_result.gas_estimate,
        })
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
        print(f"complete_unstake TX: {tx_hash.hex()}, status={receipt['status']}")
        assert receipt["status"] == 1, (
            "complete_unstake TX reverted! This is the VIB-1529 selector bug if "
            "you see the wrong selector (0x2e17de78 instead of 0xf2888dbb)."
        )

        # --- Layer 3: Parse receipt ---
        receipt_dict = dict(receipt)
        receipt_dict["logs"] = [dict(log) for log in receipt["logs"]]

        parsed = parser.parse_receipt(receipt_dict)
        print(f"Parsed receipt: {parsed}")
        assert parsed.success, f"Receipt parsing failed: {parsed.error}"
        assert parsed.withdraws, (
            "Expected a Withdraw event from sUSDe contract after complete_unstake. "
            "Parser found no Withdraw events -- the receipt parser may need updating "
            "if sUSDe.unstake(address) does not emit an ERC4626 Withdraw event."
        )
        withdrawn_assets_wei = int(parsed.withdraws[0].assets * Decimal(10**18))
        assert withdrawn_assets_wei > 0, (
            f"Withdraw event found but assets = 0: {parsed.withdraws[0]}"
        )
        print(f"Withdraw event: assets={parsed.withdraws[0].assets:.4f} USDe")

        # --- Layer 4: Balance delta (exact, relative to pre-complete_unstake baseline) ---
        usde_after = usde_contract.functions.balanceOf(funded_wallet).call()
        delta = usde_after - usde_before_complete
        print(f"USDe before complete: {usde_before_complete / 10**18:.2f}")
        print(f"USDe after:           {usde_after / 10**18:.2f}")
        print(f"USDe delta: +{delta / 10**18:.4f}")

        assert delta == withdrawn_assets_wei, (
            f"USDe balance delta mismatch. "
            f"Withdraw event reported {withdrawn_assets_wei} wei, "
            f"actual delta was {delta} wei."
        )
        print(f"SUCCESS: complete_unstake returned {delta / 10**18:.4f} USDe")
